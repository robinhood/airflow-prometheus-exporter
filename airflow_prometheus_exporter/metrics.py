import datetime
import json
import os
import pickle

from sqlalchemy import and_, func

from airflow.configuration import conf
from airflow.models import DagModel, DagRun, TaskFail, TaskInstance, XCom
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.state import State

CANARY_DAG = "canary_dag"
RETENTION_TIME = os.environ.get("PROMETHEUS_METRICS_DAYS", 28)


def get_min_date():
    utc_datetime = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    return utc_datetime - datetime.timedelta(days=RETENTION_TIME)


######################
# DAG Related Metrics
######################


@provide_session
def get_dag_state_info(session=None):
    """Number of DAG Runs with particular state."""
    dag_status_query = (
        session.query(
            DagRun.dag_id, DagRun.state, func.count(DagRun.state).label("count")
        )
        .filter(
            DagRun.execution_date > get_min_date(),
            DagRun.external_trigger == False,
            DagRun.state.isnot(None),
        )
        .group_by(DagRun.dag_id, DagRun.state)
        .subquery()
    )
    return (
        session.query(
            dag_status_query.c.dag_id,
            dag_status_query.c.state,
            dag_status_query.c.count,
            DagModel.owners,
        )
        .join(DagModel, DagModel.dag_id == dag_status_query.c.dag_id)
        .filter(DagModel.is_active == True, DagModel.is_paused == False)
        .all()
    )


@provide_session
def get_dag_duration_info(session=None):
    """Duration of successful DAG Runs."""
    max_execution_dt_query = (
        session.query(
            DagRun.dag_id, func.max(DagRun.execution_date).label("max_execution_dt")
        )
        .join(DagModel, DagModel.dag_id == DagRun.dag_id)
        .filter(
            DagModel.is_active == True,
            DagModel.is_paused == False,
            DagRun.state == State.SUCCESS,
            DagRun.end_date.isnot(None),
            DagRun.execution_date > get_min_date(),
        )
        .group_by(DagRun.dag_id)
        .subquery()
    )

    dag_start_dt_query = (
        session.query(
            max_execution_dt_query.c.dag_id,
            max_execution_dt_query.c.max_execution_dt.label("execution_date"),
            func.min(TaskInstance.start_date).label("start_date"),
        )
        .join(
            TaskInstance,
            and_(
                TaskInstance.dag_id == max_execution_dt_query.c.dag_id,
                (
                    TaskInstance.execution_date
                    == max_execution_dt_query.c.max_execution_dt
                ),
            ),
        )
        .filter(
            TaskInstance.start_date.isnot(None), TaskInstance.end_date.isnot(None)
        )
        .group_by(
            max_execution_dt_query.c.dag_id,
            max_execution_dt_query.c.max_execution_dt,
        )
        .subquery()
    )

    return (
        session.query(
            dag_start_dt_query.c.dag_id,
            dag_start_dt_query.c.start_date,
            DagRun.end_date,
        )
        .join(
            DagRun,
            and_(
                DagRun.dag_id == dag_start_dt_query.c.dag_id,
                DagRun.execution_date == dag_start_dt_query.c.execution_date,
            ),
        )
        .all()
    )


######################
# Task Related Metrics
######################


@provide_session
def get_task_state_info(session=None):
    """Number of task instances with particular state."""
    task_status_query = (
        session.query(
            TaskInstance.dag_id,
            TaskInstance.task_id,
            TaskInstance.state,
            func.count(TaskInstance.dag_id).label("value"),
        )
        .join(DagRun, TaskInstance.run_id == DagRun.run_id)
        .group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state)
        .filter(DagRun.execution_date > get_min_date())
        .subquery()
    )
    return (
        session.query(
            task_status_query.c.dag_id,
            task_status_query.c.task_id,
            task_status_query.c.state,
            task_status_query.c.value,
            DagModel.owners,
        )
        .join(DagModel, DagModel.dag_id == task_status_query.c.dag_id)
        .filter(DagModel.is_active == True, DagModel.is_paused == False)
        .all()
    )


@provide_session
def get_task_failure_counts(session=None):
    """Compute Task Failure Counts."""
    return (
        session.query(
            TaskFail.dag_id,
            TaskFail.task_id,
            func.count(TaskFail.dag_id).label("count"),
        )
        .join(DagModel, DagModel.dag_id == TaskFail.dag_id)
        .filter(DagModel.is_active == True, DagModel.is_paused == False)
        .group_by(TaskFail.dag_id, TaskFail.task_id)
    )


@provide_session
def get_xcom_params(task_id, session=None):
    """XCom parameters for matching task_id's for the latest run of a DAG."""
    max_execution_dt_query = (
        session.query(
            DagRun.dag_id, func.max(DagRun.execution_date).label("max_execution_dt")
        )
        .filter(TaskInstance.state.isnot(None))
        .group_by(DagRun.dag_id)
        .subquery()
    )

    query = session.query(XCom.dag_id, XCom.task_id, XCom.value).join(
        max_execution_dt_query,
        and_(
            (XCom.dag_id == max_execution_dt_query.c.dag_id),
            (XCom.execution_date == max_execution_dt_query.c.max_execution_dt),
        ),
    )
    if task_id == "all":
        return query.all()
    else:
        return query.filter(XCom.task_id == task_id).all()


def extract_xcom_parameter(value):
    """Deserializes value stored in XCom table."""
    enable_pickling = conf.getboolean("core", "enable_XCom_pickling")
    if enable_pickling:
        value = pickle.loads(value)
        try:
            value = json.loads(value)
            return value
        except Exception:
            return {}
    else:
        try:
            return json.loads(value.decode("UTF-8"))
        except ValueError:
            log = LoggingMixin().log
            log.error(
                "Could not deserialize the XCOM value from JSON. "
                "If you are using pickles instead of JSON "
                "for XCOM, then you need to enable pickle "
                "support for XCOM in your airflow config."
            )
            return {}


@provide_session
def get_task_duration_info(session=None):
    """Duration of successful tasks in seconds."""
    max_execution_dt_query = (
        session.query(
            DagRun.dag_id, func.max(DagRun.execution_date).label("max_execution_dt")
        )
        .join(DagModel, DagModel.dag_id == DagRun.dag_id)
        .filter(
            DagModel.is_active == True,
            DagModel.is_paused == False,
            DagRun.state == State.SUCCESS,
            DagRun.end_date.isnot(None),
            DagRun.execution_date > get_min_date(),
        )
        .group_by(DagRun.dag_id)
        .subquery()
    )

    return (
        session.query(
            TaskInstance.dag_id,
            TaskInstance.task_id,
            TaskInstance.start_date,
            TaskInstance.end_date,
            DagRun.execution_date,
        )
        .join(DagRun, TaskInstance.run_id == DagRun.run_id)
        .join(
            max_execution_dt_query,
            and_(
                (TaskInstance.dag_id == max_execution_dt_query.c.dag_id),
                (
                    TaskInstance.execution_date
                    == max_execution_dt_query.c.max_execution_dt
                ),
            ),
        )
        .filter(
            TaskInstance.state == State.SUCCESS,
            TaskInstance.start_date.isnot(None),
            TaskInstance.end_date.isnot(None),
        )
        .all()
    )


######################
# Scheduler Related Metrics
######################


@provide_session
def get_dag_scheduler_delay(session=None):
    """Compute DAG scheduling delay."""
    return (
        session.query(DagRun.dag_id, DagRun.execution_date, DagRun.start_date)
        .filter(
            DagRun.dag_id == CANARY_DAG,
            DagRun.execution_date > get_min_date(),
        )
        .order_by(DagRun.execution_date.desc())
        .limit(1)
        .all()
    )


@provide_session
def get_task_scheduler_delay(session=None):
    """Compute Task scheduling delay."""
    task_status_query = (
        session.query(
            TaskInstance.queue, func.max(TaskInstance.start_date).label("max_start")
        )
        .join(DagRun, TaskInstance.run_id == DagRun.run_id)
        .filter(
            TaskInstance.dag_id == CANARY_DAG,
            TaskInstance.queued_dttm.isnot(None),
            DagRun.execution_date > get_min_date(),
        )
        .group_by(TaskInstance.queue)
        .subquery()
    )
    return (
        session.query(
            task_status_query.c.queue,
            DagRun.execution_date,
            TaskInstance.queued_dttm,
            task_status_query.c.max_start.label("start_date"),
        )
        .join(
            TaskInstance,
            and_(
                TaskInstance.queue == task_status_query.c.queue,
                TaskInstance.start_date == task_status_query.c.max_start,
            ),
        )
        .join(DagRun, TaskInstance.run_id == DagRun.run_id)
        .filter(TaskInstance.dag_id == CANARY_DAG)  # Redundant, for performance.
        .all()
    )


@provide_session
def get_num_queued_tasks(session=None):
    """Number of queued tasks currently."""
    return (
        session.query(TaskInstance)
        .filter(
            TaskInstance.state == State.QUEUED,
            TaskInstance.execution_date > get_min_date(),
        )
        .count()
    )


@provide_session
def get_active_dag_subquery(session=None):
    return (
        session.query(
            DagModel.dag_id
        )
        .filter(
            DagModel.is_active == True,
            DagModel.is_paused == False,
        )
        .subquery()
    )


@provide_session
def get_latest_successful_dag_run(session=None):
    # Return in CSV form
    active_dag = get_active_dag_subquery()

    max_execution_date = "max_execution_date"
    query = (
        session.query(
            DagRun.dag_id,
            func.max(DagRun.execution_date).label(max_execution_date)
        )
        .select_from(DagRun)
        .join(active_dag, DagRun.dag_id == active_dag.c.dag_id)
        .filter(
            DagRun.execution_date > get_min_date(),
            DagRun.state == State.SUCCESS,
        )
        .group_by(DagRun.dag_id)
        .all()
    )

    # Column names
    yield ",".join(["dag_id", max_execution_date]) + "\n"

    for r in query:
        yield ",".join(
            [r.dag_id, r.max_execution_date.strftime("%Y-%m-%d %H:%M:%S")]
        ) + "\n"


@provide_session
def get_latest_successful_task_instance(session=None):
    # Return in CSV form
    active_dag = get_active_dag_subquery()

    max_execution_date = "max_execution_date"
    query = (
        session.query(
            TaskInstance.dag_id,
            TaskInstance.task_id,
            func.max(DagRun.execution_date).label(max_execution_date)
        )
        .select_from(TaskInstance)
        .join(DagRun, TaskInstance.run_id == DagRun.run_id)
        .join(active_dag, TaskInstance.dag_id == active_dag.c.dag_id)
        .filter(
            DagRun.execution_date > get_min_date(),
            TaskInstance.state == State.SUCCESS,
        )
        .group_by(TaskInstance.dag_id, TaskInstance.task_id)
        .all()
    )

    # Column names
    yield ",".join(["dag_id", "task_id", max_execution_date]) + "\n"

    for r in query:
        yield ",".join(
            [r.dag_id, r.task_id, r.max_execution_date.strftime("%Y-%m-%d %H:%M:%S")]
        ) + "\n"
