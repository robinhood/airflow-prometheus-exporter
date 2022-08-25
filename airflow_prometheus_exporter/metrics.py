import datetime
import json
import os
import pickle

from sqlalchemy import and_, func

from airflow.configuration import conf
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from airflow.utils.session import provide_session

CANARY_DAG = "canary_dag"
RETENTION_TIME = os.environ.get("PROMETHEUS_METRICS_DAYS", 28)
TIMEZONE_LA = "America/Los_Angeles"
MISSING = "n/a"


def get_min_date():
    utc_datetime = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    return utc_datetime - datetime.timedelta(days=RETENTION_TIME)


######################
# DAG Related Metrics
######################


@provide_session
def get_dag_state_info(dag_run, dag_model, session=None):
    """Number of DAG Runs with particular state."""
    dag_status_query = (
        session.query(
            dag_run.dag_id, dag_run.state, func.count(dag_run.state).label("count")
        )
        .filter(
            dag_run.execution_date > get_min_date(),
            dag_run.external_trigger == False,
            dag_run.state.isnot(None),
        )
        .group_by(dag_run.dag_id, dag_run.state)
        .subquery()
    )
    return (
        session.query(
            dag_status_query.c.dag_id,
            dag_status_query.c.state,
            dag_status_query.c.count,
            dag_model.owners,
        )
        .join(dag_model, dag_model.dag_id == dag_status_query.c.dag_id)
        .filter(dag_model.is_active == True, dag_model.is_paused == False)
        .all()
    )


@provide_session
def get_dag_duration_info(dag_run, dag_model, task_instance, session=None):
    """Duration of successful DAG Runs."""
    max_execution_dt_query = (
        session.query(
            dag_run.dag_id, func.max(dag_run.execution_date).label("max_execution_dt")
        )
        .join(dag_model, dag_model.dag_id == dag_run.dag_id)
        .filter(
            dag_model.is_active == True,
            dag_model.is_paused == False,
            dag_run.state == State.SUCCESS,
            dag_run.end_date.isnot(None),
            dag_run.execution_date > get_min_date(),
        )
        .group_by(dag_run.dag_id)
        .subquery()
    )

    dag_start_dt_query = (
        session.query(
            max_execution_dt_query.c.dag_id,
            max_execution_dt_query.c.max_execution_dt.label("execution_date"),
            func.min(task_instance.start_date).label("start_date"),
        )
        .join(
            task_instance,
            and_(
                task_instance.dag_id == max_execution_dt_query.c.dag_id,
                (
                    task_instance.execution_date
                    == max_execution_dt_query.c.max_execution_dt
                ),
            ),
        )
        .filter(
            task_instance.start_date.isnot(None), task_instance.end_date.isnot(None)
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
            dag_run.end_date,
        )
        .join(
            dag_run,
            and_(
                dag_run.dag_id == dag_start_dt_query.c.dag_id,
                dag_run.execution_date == dag_start_dt_query.c.execution_date,
            ),
        )
        .all()
    )


######################
# Task Related Metrics
######################


@provide_session
def get_task_state_info(dag_run, task_instance, dag_model, session=None):
    """Number of task instances with particular state."""
    task_status_query = (
        session.query(
            task_instance.dag_id,
            task_instance.task_id,
            task_instance.state,
            func.count(task_instance.dag_id).label("value"),
        )
        .join(dag_run, task_instance.run_id == dag_run.run_id)
        .group_by(task_instance.dag_id, task_instance.task_id, task_instance.state)
        .filter(dag_run.execution_date > get_min_date())
        .subquery()
    )
    return (
        session.query(
            task_status_query.c.dag_id,
            task_status_query.c.task_id,
            task_status_query.c.state,
            task_status_query.c.value,
            dag_model.owners,
        )
        .join(dag_model, dag_model.dag_id == task_status_query.c.dag_id)
        .filter(dag_model.is_active == True, dag_model.is_paused == False)
        .all()
    )


@provide_session
def get_task_failure_counts(task_fail, dag_model, session=None):
    """Compute Task Failure Counts."""
    return (
        session.query(
            task_fail.dag_id,
            task_fail.task_id,
            func.count(task_fail.dag_id).label("count"),
        )
        .join(dag_model, dag_model.dag_id == task_fail.dag_id)
        .filter(dag_model.is_active == True, dag_model.is_paused == False)
        .group_by(task_fail.dag_id, task_fail.task_id)
    )


@provide_session
def get_xcom_params(task_instance, dag_run, xcom, task_id, session=None):
    """XCom parameters for matching task_id's for the latest run of a DAG."""
    max_execution_dt_query = (
        session.query(
            dag_run.dag_id, func.max(dag_run.execution_date).label("max_execution_dt")
        )
        .filter(task_instance.state.isnot(None))
        .group_by(dag_run.dag_id)
        .subquery()
    )

    query = session.query(xcom.dag_id, xcom.task_id, xcom.value).join(
        max_execution_dt_query,
        and_(
            (xcom.dag_id == max_execution_dt_query.c.dag_id),
            (xcom.execution_date == max_execution_dt_query.c.max_execution_dt),
        ),
    )
    if task_id == "all":
        return query.all()
    else:
        return query.filter(xcom.task_id == task_id).all()


def extract_xcom_parameter(value):
    """Deserializes value stored in xcom table."""
    enable_pickling = conf.getboolean("core", "enable_xcom_pickling")
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
def get_task_duration_info(dag_model, dag_run, task_instance, session=None):
    """Duration of successful tasks in seconds."""
    max_execution_dt_query = (
        session.query(
            dag_run.dag_id, func.max(dag_run.execution_date).label("max_execution_dt")
        )
        .join(dag_model, dag_model.dag_id == dag_run.dag_id)
        .filter(
            dag_model.is_active == True,
            dag_model.is_paused == False,
            dag_run.state == State.SUCCESS,
            dag_run.end_date.isnot(None),
            dag_run.execution_date > get_min_date(),
        )
        .group_by(dag_run.dag_id)
        .subquery()
    )

    return (
        session.query(
            task_instance.dag_id,
            task_instance.task_id,
            task_instance.start_date,
            task_instance.end_date,
            dag_run.execution_date,
        )
        .join(dag_run, task_instance.run_id == dag_run.run_id)
        .join(
            max_execution_dt_query,
            and_(
                (task_instance.dag_id == max_execution_dt_query.c.dag_id),
                (
                    task_instance.execution_date
                    == max_execution_dt_query.c.max_execution_dt
                ),
            ),
        )
        .filter(
            task_instance.state == State.SUCCESS,
            task_instance.start_date.isnot(None),
            task_instance.end_date.isnot(None),
        )
        .all()
    )


######################
# Scheduler Related Metrics
######################


@provide_session
def get_dag_scheduler_delay(dag_run, session=None):
    """Compute DAG scheduling delay."""
    return (
        session.query(dag_run.dag_id, dag_run.execution_date, dag_run.start_date)
        .filter(
            dag_run.dag_id == CANARY_DAG,
            dag_run.execution_date > get_min_date(),
        )
        .order_by(dag_run.execution_date.desc())
        .limit(1)
        .all()
    )


@provide_session
def get_task_scheduler_delay(dag_run, task_instance, session=None):
    """Compute Task scheduling delay."""
    task_status_query = (
        session.query(
            task_instance.queue, func.max(task_instance.start_date).label("max_start")
        )
        .join(dag_run, task_instance.run_id == dag_run.run_id)
        .filter(
            task_instance.dag_id == CANARY_DAG,
            task_instance.queued_dttm.isnot(None),
            dag_run.execution_date > get_min_date(),
        )
        .group_by(task_instance.queue)
        .subquery()
    )
    return (
        session.query(
            task_status_query.c.queue,
            dag_run.execution_date,
            task_instance.queued_dttm,
            task_status_query.c.max_start.label("start_date"),
        )
        .join(
            task_instance,
            and_(
                task_instance.queue == task_status_query.c.queue,
                task_instance.start_date == task_status_query.c.max_start,
            ),
        )
        .join(dag_run, task_instance.run_id == dag_run.run_id)
        .filter(task_instance.dag_id == CANARY_DAG)  # Redundant, for performance.
        .all()
    )


@provide_session
def get_num_queued_tasks(task_instance, session=None):
    """Number of queued tasks currently."""
    return (
        session.query(task_instance)
        .filter(
            task_instance.state == State.QUEUED,
            task_instance.execution_date > get_min_date(),
        )
        .count()
    )


@provide_session
def get_latest_successful_dag_run(dag_model, dag_run, column_name=False, session=None):
    max_execution_date = "max_execution_date"
    latest_successful_run = (
        session.query(
            dag_run.dag_id, 
            func.max(dag_run.execution_date).label(max_execution_date)
        )
        .filter(
            dag_run.execution_date > get_min_date(),
            dag_run.external_trigger == False,
            dag_run.state == State.SUCCESS,
        )
        .group_by(dag_run.dag_id)
        .subquery()
    )

    query = (
        session.query(
            latest_successful_run.c.dag_id,
            latest_successful_run.c.max_execution_date,
        )
        .join(dag_model, dag_model.dag_id == latest_successful_run.c.dag_id)
        .filter(
            dag_model.is_active == True,
            dag_model.is_paused == False,
        )
        .all()
    )

    if column_name:
        yield ",".join(["dag_id", max_execution_date]) + "\n"
    for r in query:
        yield ",".join(
            [r.dag_id, r.max_execution_date.strftime("%Y-%m-%d %H:%M:%S")]
        ) + "\n"


@provide_session
def get_latest_successful_task_instance(
    dag_model, task_instance, column_name=False, session=None
):
    active_dag = (
        session.query(
            dag_model.dag_id
        )
        .filter(
            dag_model.is_active == True,
            dag_model.is_paused == False,
        )
        .subquery()
    )

    max_execution_date = "max_execution_date"
    query = (
        session.query(
            task_instance.dag_id,
            task_instance.task_id,
            func.max(task_instance.execution_date).label(max_execution_date)
        )
        .join(task_instance, task_instance.dag_id == active_dag.c.dag_id)
        .filter(
            task_instance.execution_date > get_min_date(),
            task_instance.state == State.SUCCESS,
        )
        .group_by(task_instance.dag_id, task_instance.task_id)
        .all()
    )

    #latest_successful_run = (
    #    session.query(
    #        task_instance.dag_id,
    #        task_instance.task_id,
    #        func.max(task_instance.execution_date).label(max_execution_date)
    #    )
    #    .filter(
    #        task_instance.execution_date > get_min_date(),
    #        task_instance.state == State.SUCCESS,
    #    )
    #    .group_by(task_instance.dag_id, task_instance.task_id)
    #    .subquery()
    #)

    #query = (
    #    session.query(
    #        latest_successful_run.c.dag_id,
    #        latest_successful_run.c.task_id,
    #        latest_successful_run.c.max_execution_date,
    #    )
    #    .join(dag_model, dag_model.dag_id == latest_successful_run.c.dag_id)
    #    .filter(
    #        dag_model.is_active == True,
    #        dag_model.is_paused == False,
    #    )
    #    .all()
    #)

    if column_name:
        yield ",".join(["dag_id", "task_id", max_execution_date]) + "\n"
    for r in query:
        yield ",".join(
            [r.dag_id, r.task_id, r.max_execution_date.strftime("%Y-%m-%d %H:%M:%S")]
        ) + "\n"
