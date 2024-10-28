#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from collections import OrderedDict
import logging

from abc import abstractmethod
import functools
import os
import textwrap
from typing import Optional, Sequence
import uuid

import docker
from utils.process_utils import execute_in_subprocess

from pandas import DataFrame
from environments.base_environment import FINISHED_DAG_RUN_STATES, Action, BaseEnvironment, State

from environments.kubernetes.remote_runner import RemoteRunner

from performance_dags.performance_dag.performance_dag_utils import (
    calculate_number_of_dag_runs,
    generate_copies_of_performance_dag,
    get_dags_count,
    get_dag_prefix,
    prepare_performance_dag_columns,
)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

DEFAULT_POD_PREFIX = "airflow-worker"
DEFAULT_CONTAINER_NAME = "airflow-worker"
AIRFLOW_NAMESPACE = "airflow"
DAGS_FOLDER = "/opt/airflow/dags/"

HELM_CHART_PATH = os.path.join(
    os.path.abspath(__file__).split(__name__.replace(".", "/"))[0],  # project root
    "airflow-subrepo",
    "chart",
)


def handle_reconciling_cluster(method):
    """
    Decorator for state methods communicating with GKE cluster via RemoteRunner, which will first
    check if the cluster is RUNNING and will immediately return the current environment state
    if it is RECONCILING.
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if not self.check_cluster_readiness():
            return self.state
        return method(self, *args, **kwargs)

    return wrapper


class BaseKubernetesEnvironment(BaseEnvironment):

    def __init__(
        self,
        namespace_prefix: str,
        pod_prefix: Optional[str] = DEFAULT_POD_PREFIX,
        container_name: Optional[str] = DEFAULT_CONTAINER_NAME,
        system_namespaces: Optional[list[str]] = None,
    ):
        # details needed to create an instance of RemoteRunnerProvider
        # are available only after environment is ready
        super().__init__()
        self.namespace_prefix = namespace_prefix
        self.system_namespaces = system_namespaces or []
        self.pod_prefix = pod_prefix
        self.container_name = container_name

    def states_map(self) -> dict[State, Action]:
        """
        Returns a map specifying a method that should be executed for every applicable state
        to move the performance test forward.
        """
        return {
            State.NONE: Action(self.prepare_k8_cluster, sleep_time=None, retryable=True),
            State.WAIT_UNTIL_READY: Action(self.is_gke_cluster_ready, sleep_time=30.0, retryable=True),
            State.WAIT_UNTIL_CAN_BE_DELETED: Action(
                self.is_gke_cluster_ready, sleep_time=30.0, retryable=True
            ),
            State.DELETING_ENV: Action(self._wait_for_deletion, sleep_time=20.0, retryable=True),
            State.UPDATE_ENV_INFO: Action(self.install_airflow, sleep_time=10.0, retryable=True),
            State.WAIT_FOR_DAG: Action(self.check_if_dags_have_loaded, sleep_time=30.0, retryable=True),
            State.UNPAUSE_DAG: Action(self.unpause_dags, sleep_time=20.0, retryable=True),
            State.WAIT_FOR_DAG_RUN_EXEC: Action(
                self.check_dag_run_execution_status, sleep_time=60.0, retryable=True
            ),
            State.COLLECT_RESULTS: Action(self.collect_results, sleep_time=10.0, retryable=True),
        }

    def install_airflow(self) -> State:
        """
        Install Airflow from helm.
        """

        # create an instance of kubernetes api
        self.remote_runner_provider = self.create_remote_runner_provider()

        # TODO: this method is not retryable - make separate state out of it
        self.install_airflow_from_helm_chart()

        return State.WAIT_FOR_DAG

    def install_airflow_from_helm_chart(self) -> None:
        """
        Prepares docker image and installs Apache Airflow on given GKE cluster.
        """
        if self.docker_image:
            # TODO: when reusing an image we cannot be sure how many elastic dag copies
            #  are present there and if that number matches provided elastic dag configuration
            log.info("Using specified docker image: %s", self.docker_image)
            # tag "latest" seems not to work when provided to helm chart
            image_tag = self.docker_image.split(":")[-1]
        else:
            log.info("Preparing new docker image.")
            image_tag = self.publish_new_airflow_image()

        helm_install_command = self.prepare_helm_install_command(image_tag)

        # create namespace for Airflow deployment
        with self.remote_runner_provider.get_kubernetes_apis_in_isolated_context() as (
            core_api,
            _,
            proxy,
        ):
            # TODO: this needs handling reconciling cluster
            self.remote_runner_provider.create_namespace(core_api, AIRFLOW_NAMESPACE)

            execute_in_subprocess(["helm", "repo", "add", "stable", "https://charts.helm.sh/stable/"])

            execute_in_subprocess(["helm", "dep", "update", HELM_CHART_PATH])

            if proxy:
                helm_install_command = [f"https_proxy={proxy}"] + helm_install_command
            execute_in_subprocess(" ".join(helm_install_command))

    def publish_new_airflow_image(self) -> str:
        """
        Builds a new version of Airflow image with dag files uploaded to it and publishes it to
        'performance_dag' gcr repository.
        """
        container_repository = self.get_container_repository()

        new_image_tag = str(uuid.uuid4())

        image_name = f"{container_repository}:{new_image_tag}"

        # TODO: what about specifying python version? there are differences in minor python version
        #  between helm and composer

        # TODO: with helm, worker pods do not have to be on separate nodes like in composer
        dockerfile_contents = textwrap.dedent(
            """
        FROM apache/airflow:{airflow_image_tag}
        """
        ).format(airflow_image_tag=self.airflow_image_tag)

        docker_client = docker.from_env()

        with generate_copies_of_performance_dag(self.performance_dag_path, self.get_env_variables()) as (
            temp_dir,
            performance_dag_copies,
        ):

            for file_path in performance_dag_copies:

                file_name = os.path.basename(file_path)

                dockerfile_contents += textwrap.dedent(
                    """
                COPY {file_name} {dags_folder}
                """
                ).format(file_name=file_name, dags_folder=DAGS_FOLDER)

            with open(os.path.join(temp_dir, "Dockerfile"), "w") as dockerfile:
                dockerfile.write(dockerfile_contents)

            log.info("Building image: %s", image_name)
            docker_client.images.build(path=temp_dir, rm=True, tag=image_name)

        log.info("Pushing image: %s", image_name)
        docker_client.images.push(image_name)

        return new_image_tag

    def prepare_helm_install_command(self, image_tag: str) -> list[str]:
        """
        Prepares a list with arguments for helm subprocess command that will install Airflow
        on the GKE cluster and set environment variables on it.

        :param image_tag: tag of the image to use in installation.
        :type image_tag: str

        :return: a list of arguments for helm install command.
        :rtype: List[str]
        """

        def get_helm_env_var_setter_flag(index, env_var_name, env_var_value):
            env_var_value = str(env_var_value).replace(",", "\\,")
            return [
                "--set",
                f'env[{index}].name="{env_var_name}",env[{index}].value="{env_var_value}"',
            ]

        helm_install_command = [
            "helm",
            "install",
            "airflow",
            HELM_CHART_PATH,
            "--namespace",
            "airflow",
        ]

        default_helm_chart_sets = {
            "executor": "CeleryExecutor",
            "pgbouncer.enabled": "true",
            "pgbouncer.maxClientConn": "1000",
            "workers.replicas": str(self.get_node_count()),
            # TODO: verify if disabling persistence on workers is fine
            "workers.persistence.enabled": "false",
            "images.airflow.repository": self.get_container_repository(),
            "images.airflow.tag": image_tag,
        }

        # replace the default helm chart overwrites
        # with the ones provided explicitly in specification
        self.helm_chart_sets = {**default_helm_chart_sets, **self.helm_chart_sets}

        for config_option, value in self.helm_chart_sets.items():
            helm_install_command += ["--set", f"{config_option}={value}"]

        # prepare environment variable sets
        for env_var_index, env_var in enumerate(self.env_variable_sets.items()):
            helm_install_command += get_helm_env_var_setter_flag(env_var_index, env_var[0], env_var[1])

        return helm_install_command

    @abstractmethod
    def prepare_k8_cluster(self) -> State:
        pass

    @abstractmethod
    def is_gke_cluster_ready(self) -> State:
        pass

    @abstractmethod
    def collect_results(self) -> State:
        pass

    @abstractmethod
    def create_remote_runner_provider(self):
        pass

    @handle_reconciling_cluster
    def check_if_dags_have_loaded(self) -> State:
        """
        Checks if expected DAGs have already been parsed by scheduler. Moves to the next state
        if all DAGs are present in Airflow database.
        """

        dag_id_prefix = self.get_dag_prefix()

        expected_dags_count = self.get_dags_count()

        with self.remote_runner_provider.get_remote_runner(
            pod_prefix=self.pod_prefix, container=self.container_name
        ) as runner:
            number_of_parsed_dags = runner.get_dags_count(dag_id_prefix=dag_id_prefix)

        if number_of_parsed_dags < expected_dags_count:
            log.info(
                "Not all expected DAGs are present on environment %s. " "DAGs parsed: %d/%d. ",
                self.name,
                number_of_parsed_dags,
                expected_dags_count,
            )
            return self.state

        log.info("All expected DAGs on environment %s have been parsed.", self.name)
        return State.UNPAUSE_DAG

    @handle_reconciling_cluster
    def unpause_dags(self) -> State:
        """
        Unpauses the test dags and moves to the next state.
        """

        dag_id_prefix = self.get_dag_prefix()

        log.info("Unpausing test DAGs on environment %s.", self.name)

        with self.remote_runner_provider.get_remote_runner(
            pod_prefix=self.pod_prefix, container=self.container_name
        ) as runner:
            runner.unpause_dags(dag_id_prefix=dag_id_prefix)

        return State.WAIT_FOR_DAG_RUN_EXEC

    @handle_reconciling_cluster
    def check_dag_run_execution_status(self) -> State:
        """
        Checks if all test Dag Runs finished their execution.
        """

        dag_id_prefix = self.get_dag_prefix()
        expected_dag_runs_count = self.get_expected_dag_runs_count()

        with self.remote_runner_provider.get_remote_runner(
            pod_prefix=self.pod_prefix, container=self.container_name
        ) as runner:
            finished_dag_runs_count = runner.get_dag_runs_count(
                dag_id_prefix=dag_id_prefix, states=FINISHED_DAG_RUN_STATES
            )
        if finished_dag_runs_count < expected_dag_runs_count:
            log.info(
                "Dag runs are still executing on environment %s. " "Dag runs: %d/%d. ",
                self.name,
                finished_dag_runs_count,
                expected_dag_runs_count,
            )
            return self.state
        log.info("All expected Dag runs on environment %s have finished.", self.name)
        return State.COLLECT_RESULTS

    def collect_airflow_statistics(self, runner: RemoteRunner) -> OrderedDict:
        """
        Collects statistics of finished test Dag Runs.

        :param runner: an instance of RemoteRunner class which can be used to execute
            remote commands on GKE cluster of given environment.
        :type runner: RemoteRunner

        :return: dictionary with test statistics
            (like start and end time, average Dag Run duration).
        :rtype: OrderedDict
        """

        dag_id_prefix = self.get_dag_prefix()

        test_statistics = runner.collect_dag_run_statistics(
            dag_id_prefix=dag_id_prefix, states=FINISHED_DAG_RUN_STATES
        )

        return test_statistics

    def collect_disk_ids(self) -> list[str]:
        """
        Collects and returns ids of disks assigned to cluster's nodes.
        """

        # this should be doable even if cluster is reconciling
        list_nodes_result = self.remote_runner_provider.list_nodes()
        gke_node_urls = [node.get("instance") for node in list_nodes_result["items"]]

        disks = (
            self.compute_client.disks().list(project=self.get_project_id(), zone=self.get_zone()).execute()
        )

        gke_cluster_disk_ids = []
        for disk in disks.get("items", []):
            if any(node_url for node_url in gke_node_urls if disk.get("users") == [node_url]):
                gke_cluster_disk_ids.append(disk["id"])

        return gke_cluster_disk_ids

    def collect_python_version(self) -> str:
        """
        Collects version of python used on this environment using RemoteRunner.
        """
        with self.remote_runner_provider.get_remote_runner(
            pod_prefix=self.pod_prefix, container=self.container_name
        ) as runner:
            python_version = runner.get_python_version()

        return python_version

    def collect_airflow_version(self) -> str:
        """
        Collects version of airflow used on this environment using RemoteRunner.
        """
        with self.remote_runner_provider.get_remote_runner(
            pod_prefix=self.pod_prefix, container=self.container_name
        ) as runner:
            airflow_version = runner.get_airflow_version()

        return airflow_version

    def get_gke_cluster_state(self) -> str:
        """
        Returns the current state of the GKE cluster.

        :return: string with current state of the cluster.
        :rtype: str
        """
        return self.get_gke_cluster().status

    def get_dag_prefix(self) -> str:
        """
        Gets a string that should be a prefix for every test DAG's dag_id.
        This allows to find Dag Runs of said test DAGs.
        """

        dag_prefix = get_dag_prefix(self.get_env_variables())

        return dag_prefix

    def get_dags_count(self) -> int:
        """
        Gets the number of test DAGs.
        """

        dags_count = get_dags_count(self.get_env_variables())

        return dags_count

    def get_expected_dag_runs_count(self) -> int:
        """
        Gets a total number of test Dag Runs that are expected to be triggered
        on given environment.
        This allows to tell when the tests have finished.
        """

        number_of_dag_runs = calculate_number_of_dag_runs(self.get_env_variables())

        return number_of_dag_runs

    def prepare_performance_dag_columns(self) -> OrderedDict:
        """
        Prepares an OrderedDict containing elastic dag configuration that will serve as columns
        for the results dataframe.

        :return: a dict with elastic dag configuration environment variables
            in order in which they should appear in the results dataframe.
        :rtype: OrderedDict
        """

        return prepare_performance_dag_columns(self.get_env_variables())

    def get_environment_size(self) -> str:
        """
        Checks the size of the GKE cluster and returns a matching pre-defined size category.
        """
        environment_sizes = {
            "small": {
                "node_count": 3,
                "disk_size_gb": 100,
                "machine_type": "n1-standard-2",
            },
            "medium": {
                "node_count": 6,
                "disk_size_gb": 200,
                "machine_type": "n1-standard-4",
            },
            "big": {
                "node_count": 12,
                "disk_size_gb": 400,
                "machine_type": "n1-standard-8",
            },
        }

        environment_size = {
            "node_count": self.get_node_count(),
            "disk_size_gb": self.get_disk_size(),
            "machine_type": self.get_machine_type(),
        }

        for size in environment_sizes:
            if environment_size == environment_sizes[size]:
                return size

        return "custom"

    def prepare_environment_columns(self) -> OrderedDict:
        """
        This method should return an OrderedDict containing environment configuration.
        """
        raise NotImplementedError

    def get_project_id(self) -> str:
        """
        This method should return the project id the environment belongs to.
        """
        raise NotImplementedError

    def get_zone(self) -> str:
        """
        This method should return the zone the environment belongs to.
        """
        raise NotImplementedError

    def get_gke_cluster_name(self) -> str:
        """
        This method should return the full name of the GKE cluster the environment runs on.
        """
        raise NotImplementedError

    def get_results_object_name_components(self, results_df: DataFrame) -> Sequence[str]:
        """
        This method should return a sequence of components from which a results object's name can
        be formed.
        """
        raise NotImplementedError

    def get_env_variables(self) -> dict[str, str]:
        """
        This method should return a dictionary with environment variables set on given environment.
        """
        raise NotImplementedError

    def get_node_count(self) -> int:
        """
        This method should return the number of nodes belonging to the GKE cluster.
        """
        raise NotImplementedError

    def get_disk_size(self) -> int:
        """
        This method should return a size of disks assigned to the GKE cluster's nodes.
        """
        raise NotImplementedError

    def get_machine_type(self) -> str:
        """
        This method should return the type of machine used as the GKE cluster's nodes.
        """
        raise NotImplementedError
