from __future__ import annotations
from functools import cached_property
from operator import or_
from typing import TYPE_CHECKING
from airflow.job_state.job_state_manager import JobStateManager
from airflow.jobs.job import Job
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.configuration import conf
from airflow.utils.timezone import timezone
from airflow.utils.state import JobState, State, TaskInstanceState
from airflow.models.base import ID_LEN, Base

from sqlalchemy import Column, Index, Integer, String, case, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import backref, foreign, relationship
from sqlalchemy.orm.session import make_transient
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    import datetime

    from sqlalchemy.orm.session import Session

TI = TaskInstance
DR = DagRun
DM = DagModel


class DatabaseJobStateManager(JobStateManager, LoggingMixin):
    __tablename__ = "job"

    id = Column(Integer, primary_key=True)
    dag_id = Column(
        String(ID_LEN),
    )
    state = Column(String(20))
    job_type = Column(String(30))
    start_date = Column(UtcDateTime())
    end_date = Column(UtcDateTime())
    latest_heartbeat = Column(UtcDateTime())
    executor_class = Column(String(500))
    hostname = Column(String(500))
    unixname = Column(String(1000))

    __table_args__ = (
        Index("job_type_heart", job_type, latest_heartbeat),
        Index("idx_job_state_heartbeat", state, latest_heartbeat),
        Index("idx_job_dag_id", dag_id),
    )

    task_instances_enqueued = relationship(
        "TaskInstance",
        primaryjoin="Job.id == foreign(TaskInstance.queued_by_job_id)",
        backref=backref("queued_by_job", uselist=False),
    )

    dag_runs = relationship(
        "DagRun",
        primaryjoin=lambda: Job.id == foreign(_resolve_dagrun_model().creating_job_id),
        backref="creating_job",
    )

    def __init__(
        self,
        job_type: str | None,
        heartrate: float,
        grace_multiplier: float = 2.1,
    ):
        self.job_type = job_type
        self.heartrate = heartrate
        self.grace_multiplier = grace_multiplier

    def get_job_state(self, job_id: str) -> JobState:
        pass

    @cached_property
    def heartrate(self) -> float:
        return DatabaseJobStateManager._heartrate(self.job_type)

    @staticmethod
    def _heartrate(job_type: str) -> float:
        if job_type == "TriggererJob":
            return conf.getfloat("triggerer", "JOB_HEARTBEAT_SEC")
        else:
            # Heartrate used to be hardcoded to scheduler, so in all other
            # cases continue to use that value for back compat
            return conf.getfloat("scheduler", "JOB_HEARTBEAT_SEC")

    def perform_heartbeat(self, job_id, heartbeat_callback):
        """
        Perform heartbeat for the Job passed to it,optionally checking if it is necessary.

        :param job: job to perform heartbeat for
        :param heartbeat_callback: callback to run by the heartbeat
        """
        seconds_remaining: float = 0.0
        if job.latest_heartbeat and job.heartrate:
            seconds_remaining = job.heartrate - (timezone.utcnow() - job.latest_heartbeat).total_seconds()
        if seconds_remaining > 0 and only_if_necessary:
            return
        job.heartbeat(heartbeat_callback=heartbeat_callback)

    @provide_session
    def _perform_heartbeat(self, job_id, heartbeat_callback, session: Session = NEW_SESSION):
        pass

    @provide_session
    def _latest_heartbeat(self, session: Session = NEW_SESSION):
        pass

    def is_alive(self):
        health_check_threshold: float
        if self.job_type == "SchedulerJob":
            health_check_threshold = conf.getint("scheduler", "scheduler_health_check_threshold")
        elif self.job_type == "TriggererJob":
            health_check_threshold = conf.getint("triggerer", "triggerer_health_check_threshold")
        else:
            health_check_threshold = self.heartrate * self.grace_multiplier
        return (timezone.utcnow() - latest_heartbeat).total_seconds() < health_check_threshold


def find_zombie_tasks(limit_dttm, scheduled_job_id):
    with create_session() as session:
        zombies: list[tuple[TI, str, str]] = (
            session.execute(
                select(TI, DM.fileloc, DM.processor_subdir)
                .with_hint(TI, "USE INDEX (ti_state)", dialect_name="mysql")
                .join(Job, TI.job_id == Job.id)
                .join(DM, TI.dag_id == DM.dag_id)
                .where(TI.state == TaskInstanceState.RUNNING)
                .where(
                    or_(
                        Job.state != JobState.RUNNING,
                        Job.latest_heartbeat < limit_dttm,
                    )
                )
                .where(Job.job_type == "LocalTaskJob")
                .where(TI.queued_by_job_id == scheduled_job_id)
            )
            .unique()
            .all()
        )
    return zombies
