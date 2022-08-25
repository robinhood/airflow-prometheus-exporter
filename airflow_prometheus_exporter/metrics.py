import datetime
import json
import os
import pickle

from dateutil import tz
from pytimeparse import parse as pytime_parse
from sqlalchemy import Column, String, Text, Boolean, and_, func
from sqlalchemy.sql.expression import null

from airflow.configuration import conf
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from airflow.utils.session import provide_session

CANARY_DAG = "canary_dag"
RETENTION_TIME = os.environ.get("PROMETHEUS_METRICS_DAYS", 21)
TIMEZONE = conf.get("core", "default_timezone")
TIMEZONE_LA = "America/Los_Angeles"
MISSING = "n/a"


def sla_check(sla_interval, sla_time, max_execution_date, latest_sla_miss_state):
    utc_datetime = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

    if sla_time:
        # Convert user defined SLA time to local datetime.
        local_datetime = utc_datetime.astimezone(tz.gettz(TIMEZONE_LA))
        sla_datetime = datetime.datetime.combine(
            local_datetime.date(),
            datetime.datetime.strptime(sla_time, "%H:%M").time(),
        ).replace(tzinfo=tz.gettz(TIMEZONE_LA))
    else:
        # If no defined SLA time, meaning always run SLA check.
        sla_datetime = utc_datetime

    interval_in_second = pytime_parse(sla_interval)
    checkpoint = sla_datetime - datetime.timedelta(seconds=interval_in_second)

    # Check SLA miss when it's SLA time.
    if utc_datetime >= sla_datetime:
        return max_execution_date < checkpoint

    # Use latest_sla_miss_state if it's before SLA time. Default to False
    return latest_sla_miss_state or False


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
def upsert_auxiliary_info(delay_alert_auxiliary_info, upsert_dict, session=None):
    for k, v in upsert_dict.items():
        dag_id, task_id, sla_interval, sla_time = k
        value = v["value"]
        insert = v["insert"]
        latest_successful_run = value["max_execution_date"]
        latest_sla_miss_state = value["sla_miss"]

        if insert:
            session.add(
                delay_alert_auxiliary_info(
                    dag_id=dag_id,
                    task_id=task_id,
                    sla_interval=sla_interval,
                    sla_time=sla_time,
                    latest_successful_run=latest_successful_run,
                    latest_sla_miss_state=latest_sla_miss_state,
                )
            )
        else:
            session.query(delay_alert_auxiliary_info).filter(
                delay_alert_auxiliary_info.dag_id == dag_id,
                delay_alert_auxiliary_info.task_id == task_id,
                delay_alert_auxiliary_info.sla_interval == sla_interval,
                delay_alert_auxiliary_info.sla_time == sla_time,
            ).update(
                {
                    delay_alert_auxiliary_info.latest_successful_run: latest_successful_run,
                    delay_alert_auxiliary_info.latest_sla_miss_state: latest_sla_miss_state,
                }
            )
    session.flush()
    session.commit()


@provide_session
def get_latest_successful_dag_run(dag_model, dag_run, colum_name=False, session=None):
    latest_successful_run = (
        session.query(dag_run.dag_id, dag_run.execution_date)
        .add_column(
            func.row_number()
            .over(partition_by=dag_run.dag_id, order_by=desc(dag_run.execution_date))
            .label("row_number_column")
        )
        .filter(
            dag_run.execution_date > get_min_date(),
            dag_run.external_trigger == False,
            dag_run.state == "success",
            dag_run.row_number_column == 1,
        )
        .subquery()
    )

    query = (
        session.query(
            latest_successful_run.c.dag_id,
            latest_successful_run.c.execution_date,
        )
        .join(dag_model, dag_model.dag_id == latest_successful_run.c.dag_id)
        .filter(dag_model.is_active == True, dag_model.is_paused == False)
        .all()
    )

    if column_name:
        yield ",".join(["dag_id", "execution_date"])
    for r in query:
        yield ",".join([r.dag_id, r.execution_date])


@provide_session
def get_sla_miss(
    delay_alert_metadata,
    delay_alert_auxiliary_info,
    dag_model,
    dag_run,
    task_instance,
    session=None,
):
    active_alert_query = (
        session.query(
            delay_alert_metadata.dag_id,
            delay_alert_metadata.task_id,
            delay_alert_metadata.sla_interval,
            delay_alert_metadata.sla_time,
        )
        .join(dag_model, delay_alert_metadata.dag_id == dag_model.dag_id)
        .filter(
            delay_alert_metadata.ready == True,
            dag_model.is_active == True,
            dag_model.is_paused == False,
        )
        .group_by(
            delay_alert_metadata.dag_id,
            delay_alert_metadata.task_id,
            delay_alert_metadata.sla_interval,
            delay_alert_metadata.sla_time,
        )
        .subquery()
    )

    # Gather the current max execution dates
    dag_max_execution_date = (
        session.query(
            dag_run.dag_id,
            null().label("task_id"),
            func.max(dag_run.execution_date).label("execution_date"),
        )
        .join(
            active_alert_query,
            (dag_run.dag_id == active_alert_query.c.dag_id)
            & (active_alert_query.c.task_id.is_(None)),
        )
        .filter(
            dag_run.state == State.SUCCESS,
            dag_run.end_date.isnot(None),
            dag_run.execution_date > get_min_date(),
        )
        .group_by(dag_run.dag_id)
    )

    all_max_execution_date = (
        session.query(
            task_instance.dag_id,
            task_instance.task_id,
            func.max(dag_run.execution_date).label("execution_date"),
        )
        .join(
            active_alert_query,
            (task_instance.dag_id == active_alert_query.c.dag_id)
            & (task_instance.task_id == active_alert_query.c.task_id),
        )
        .join(dag_run, task_instance.run_id == dag_run.run_id)
        .filter(
            task_instance.state == State.SUCCESS,
            task_instance.end_date.isnot(None),
            task_instance.execution_date > get_min_date(),
        )
        .group_by(
            task_instance.dag_id,
            task_instance.task_id,
        )
        .union(dag_max_execution_date)
    )

    max_execution_dates = {}
    for r in all_max_execution_date:
        key = (r.dag_id, r.task_id)
        max_execution_dates[key] = r.execution_date

    # Getting all alerts with auxiliary data
    alert_query = (
        session.query(
            delay_alert_metadata.dag_id,
            delay_alert_metadata.task_id,
            delay_alert_metadata.sla_interval,
            delay_alert_metadata.sla_time,
            delay_alert_metadata.affected_pipeline,
            delay_alert_metadata.alert_target,
            delay_alert_metadata.alert_name,
            delay_alert_metadata.group_title,
            delay_alert_metadata.inhibit_rule,
            delay_alert_metadata.link,
            delay_alert_auxiliary_info.latest_successful_run,
            delay_alert_auxiliary_info.latest_sla_miss_state,
        )
        .join(
            active_alert_query,
            and_(
                delay_alert_metadata.dag_id == active_alert_query.c.dag_id,
                func.coalesce(delay_alert_metadata.task_id, "n/a")
                == func.coalesce(active_alert_query.c.task_id, "n/a"),
                delay_alert_metadata.sla_interval == active_alert_query.c.sla_interval,
                func.coalesce(delay_alert_metadata.sla_time, "n/a")
                == func.coalesce(active_alert_query.c.sla_time, "n/a"),
            ),
        )
        .join(
            delay_alert_auxiliary_info,
            and_(
                delay_alert_metadata.dag_id == delay_alert_auxiliary_info.dag_id,
                func.coalesce(delay_alert_metadata.task_id, "n/a")
                == func.coalesce(delay_alert_auxiliary_info.task_id, "n/a"),
                delay_alert_metadata.sla_interval
                == delay_alert_auxiliary_info.sla_interval,
                func.coalesce(delay_alert_metadata.sla_time, "n/a")
                == func.coalesce(delay_alert_auxiliary_info.sla_time, "n/a"),
            ),
            isouter=True,
        )
    )

    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc)
    upsert_dict = {}
    for alert in alert_query:
        key = (alert.dag_id, alert.task_id)
        insert = update = False

        max_execution_date = max_execution_dates.get(key, epoch)
        if alert.latest_successful_run is None:
            insert = True
        elif max_execution_date > alert.latest_successful_run:
            update = True
        else:
            max_execution_date = alert.latest_successful_run

        sla_miss = sla_check(
            alert.sla_interval,
            alert.sla_time,
            max_execution_date,
            alert.latest_sla_miss_state,
        )

        if insert or update or sla_miss != alert.latest_sla_miss_state:
            upsert_dict_key = key + (alert.sla_interval, alert.sla_time)
            upsert_dict[upsert_dict_key] = {
                "value": {
                    "max_execution_date": max_execution_date,
                    "sla_miss": sla_miss,
                },
                "insert": insert,
            }

        alert_name = alert.alert_name
        if alert_name is None:
            alert_name = alert.dag_id
            if alert.task_id:
                alert_name += "." + alert.task_id

        if sla_miss:
            yield {
                "dag_id": alert.dag_id,
                "task_id": alert.task_id or MISSING,
                "affected_pipeline": alert.affected_pipeline or MISSING,
                "alert_name": alert_name,
                "alert_target": alert.alert_target or MISSING,
                "group_title": alert.group_title or alert_name,
                "inhibit_rule": alert.inhibit_rule or MISSING,
                "link": alert.link or MISSING,
                "sla_interval": alert.sla_interval,
                "sla_miss": sla_miss,
                "sla_time": alert.sla_time or MISSING,
            }

    upsert_auxiliary_info(delay_alert_auxiliary_info, upsert_dict)


@provide_session
def get_unmonitored_dag(dag_model, delay_alert_metadata, session=None):
    query = (
        session.query(
            dag_model.dag_id,
        )
        .join(
            delay_alert_metadata,
            dag_model.dag_id == delay_alert_metadata.dag_id,
            isouter=True,
        )
        .filter(
            dag_model.is_active == True,
            dag_model.is_paused == False,
            delay_alert_metadata.dag_id.is_(None),
        )
        .group_by(dag_model.dag_id)
    )

    for r in query:
        yield r
