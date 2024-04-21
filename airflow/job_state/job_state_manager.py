import datetime
from functools import cached_property
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import JobState


class JobStateManager(LoggingMixin):
    def __init__(self, heartrate=None):
        if heartrate is not None:
            self.heartrate = heartrate

    @cached_property
    def heartrate(self) -> float:
        return JobStateManager._heartrate(self.job_type)

    def _heartrate(job_type: str) -> float:
        if job_type == "TriggererJob":
            return conf.getfloat("triggerer", "JOB_HEARTBEAT_SEC")
        else:
            # Heartrate used to be hardcoded to scheduler, so in all other
            # cases continue to use that value for back compat
            return conf.getfloat("scheduler", "JOB_HEARTBEAT_SEC")

    def create_job():
        pass

    def prepare_for_execution(self, job_id):
        pass

    def perform_heartbeat(self, job_id, heartbeat_callback):
        """
        Perform heartbeat for the Job passed to it,optionally checking if it is necessary.

        :param job: job to perform heartbeat for
        :param heartbeat_callback: callback to run by the heartbeat
        """

        pass

    @staticmethod
    def _is_alive(
        job_type: str | None,
        heartrate: float,
        state: JobState | str | None,
        latest_heartbeat: datetime.datetime,
        grace_multiplier: float = 2.1,
    ) -> bool:
        health_check_threshold: float
        if job_type == "SchedulerJob":
            health_check_threshold = conf.getint("scheduler", "scheduler_health_check_threshold")
        elif job_type == "TriggererJob":
            health_check_threshold = conf.getint("triggerer", "triggerer_health_check_threshold")
        else:
            health_check_threshold = heartrate * grace_multiplier
        return (
            state == JobState.RUNNING
            and (datetime.timezone.utcnow() - latest_heartbeat).total_seconds() < health_check_threshold
        )

    @staticmethod
    def _heartrate(job_type: str) -> float:
        if job_type == "TriggererJob":
            return conf.getfloat("triggerer", "JOB_HEARTBEAT_SEC")
        else:
            # Heartrate used to be hardcoded to scheduler, so in all other
            # cases continue to use that value for back compat
            return conf.getfloat("scheduler", "JOB_HEARTBEAT_SEC")

    def is_alive(self, job_id, job_type: str | None) -> bool:
        pass

    def get_job_state(self, job_id):
        pass

    def complete_execution(self, job_id):
        pass

    def fail_execution(self, job_id):
        pass

    def find_zombie_tasks(limit_dttm):
        pass
