"""Prometheus exporter for Airflow."""
import datetime
import json
import os
import pickle
import pytz
from contextlib import contextmanager

import dateparser
import pendulum
from flask import Response
from flask_admin import BaseView, expose
from prometheus_client import REGISTRY, generate_latest
from prometheus_client.core import GaugeMetricFamily
from pytimeparse import parse as pytime_parse
from sqlalchemy import Column, String, and_, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utcdatetime import UTCDateTime

from airflow.configuration import conf
from airflow.models import DagModel, DagRun, TaskFail, TaskInstance, XCom
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import RBAC, Session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from airflow_prometheus_exporter.xcom_config import load_xcom_config

CANARY_DAG = "canary_dag"
RETENTION_TIME = os.environ.get("PROMETHEUS_METRICS_DAYS", 21)
TIMEZONE = conf.get("core", "default_timezone")
TIMEZONE_LA = "America/Los_Angeles"
MISSING = "missing"


@contextmanager
def session_scope(session):
    """Provide a transactional scope around a series of operations."""
    try:
        yield session
    finally:
        session.close()


with session_scope(Session) as session:
    Base = declarative_base(session.get_bind())

    class DelayAlertMetaData(Base):
        __tablename__ = "delay_alert_metadata"
        __table_args__ = {"autoload": True}
        dag_id = Column(String, primary_key=True)
        latest_successful_run = Column(UTCDateTime)


######################
# DAG Related Metrics
######################


def get_dag_state_info():
    """Number of DAG Runs with particular state."""
    with session_scope(Session) as session:
        min_date_to_filter = pendulum.now(TIMEZONE).subtract(days=RETENTION_TIME)
        dag_status_query = (
            session.query(
                DagRun.dag_id, DagRun.state, func.count(DagRun.state).label("count")
            )
            .filter(
                DagRun.execution_date > min_date_to_filter,
                DagRun.external_trigger == False,
                DagRun.state.isnot(None),
            )  # noqa
            .group_by(DagRun.dag_id, DagRun.state)
            .subquery()
        )
        return (
            session.query(
                dag_status_query.c.dag_id,
                dag_status_query.c.state,
                dag_status_query.c.count,
                DagModel.owners,
                DelayAlertMetaData.cadence,
                DelayAlertMetaData.severity,
                DelayAlertMetaData.alert_target,
                DelayAlertMetaData.alert_external_classification,
                DelayAlertMetaData.alert_report_classification,
                DelayAlertMetaData.sla_time,
            )
            .join(DagModel, DagModel.dag_id == dag_status_query.c.dag_id)
            .filter(DagModel.is_active == True, DagModel.is_paused == False)  # noqa
            .outerjoin(
                DelayAlertMetaData,
                DelayAlertMetaData.dag_id == dag_status_query.c.dag_id,
            )
            .filter(DelayAlertMetaData.task_id.is_(None))
            .all()
        )


def get_dag_duration_info():
    """Duration of successful DAG Runs."""
    min_date_to_filter = pendulum.now(TIMEZONE).subtract(days=RETENTION_TIME)
    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id, func.max(DagRun.execution_date).label("max_execution_dt")
            )
            .join(DagModel, DagModel.dag_id == DagRun.dag_id)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
                DagRun.state == State.SUCCESS,
                DagRun.end_date.isnot(None),
                DagRun.execution_date > min_date_to_filter,
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
                        == max_execution_dt_query.c.max_execution_dt  # noqa
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


def get_task_state_info():
    """Number of task instances with particular state."""
    min_date_to_filter = pendulum.now(TIMEZONE).subtract(days=RETENTION_TIME)
    with session_scope(Session) as session:
        task_status_query = (
            session.query(
                TaskInstance.dag_id,
                TaskInstance.task_id,
                TaskInstance.state,
                func.count(TaskInstance.dag_id).label("value"),
            )
            .group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state)
            .filter(TaskInstance.execution_date > min_date_to_filter)
            .subquery()
        )
        return (
            session.query(
                task_status_query.c.dag_id,
                task_status_query.c.task_id,
                task_status_query.c.state,
                task_status_query.c.value,
                DagModel.owners,
                DelayAlertMetaData.cadence,
                DelayAlertMetaData.severity,
                DelayAlertMetaData.alert_target,
                DelayAlertMetaData.alert_external_classification,
                DelayAlertMetaData.alert_report_classification,
                DelayAlertMetaData.sla_time,
            )
            .join(DagModel, DagModel.dag_id == task_status_query.c.dag_id)
            .filter(DagModel.is_active == True, DagModel.is_paused == False)  # noqa
            .outerjoin(
                DelayAlertMetaData,
                (DelayAlertMetaData.dag_id == task_status_query.c.dag_id)
                & (DelayAlertMetaData.task_id == task_status_query.c.task_id),  # noqa
            )
            .filter(DelayAlertMetaData.task_id.isnot(None))
            .all()
        )


def get_task_failure_counts():
    """Compute Task Failure Counts."""
    with session_scope(Session) as session:
        return (
            session.query(
                TaskFail.dag_id,
                TaskFail.task_id,
                func.count(TaskFail.dag_id).label("count"),
            )
            .join(DagModel, DagModel.dag_id == TaskFail.dag_id)
            .filter(DagModel.is_active == True, DagModel.is_paused == False)  # noqa
            .group_by(TaskFail.dag_id, TaskFail.task_id)
        )


def get_xcom_params(task_id):
    """XCom parameters for matching task_id's for the latest run of a DAG."""
    with session_scope(Session) as session:
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


def get_task_duration_info():
    """Duration of successful tasks in seconds."""
    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id, func.max(DagRun.execution_date).label("max_execution_dt")
            )
            .join(DagModel, DagModel.dag_id == DagRun.dag_id)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
                DagRun.state == State.SUCCESS,
                DagRun.end_date.isnot(None),
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
                TaskInstance.execution_date,
            )
            .join(
                max_execution_dt_query,
                and_(
                    (TaskInstance.dag_id == max_execution_dt_query.c.dag_id),
                    (
                        TaskInstance.execution_date
                        == max_execution_dt_query.c.max_execution_dt  # noqa
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


def get_dag_scheduler_delay():
    """Compute DAG scheduling delay."""
    with session_scope(Session) as session:
        return (
            session.query(DagRun.dag_id, DagRun.execution_date, DagRun.start_date)
            .filter(DagRun.dag_id == CANARY_DAG)
            .order_by(DagRun.execution_date.desc())
            .limit(1)
            .all()
        )


def get_task_scheduler_delay():
    """Compute Task scheduling delay."""
    with session_scope(Session) as session:
        task_status_query = (
            session.query(
                TaskInstance.queue, func.max(TaskInstance.start_date).label("max_start")
            )
            .filter(
                TaskInstance.dag_id == CANARY_DAG, TaskInstance.queued_dttm.isnot(None)
            )
            .group_by(TaskInstance.queue)
            .subquery()
        )
        return (
            session.query(
                task_status_query.c.queue,
                TaskInstance.execution_date,
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
            .filter(TaskInstance.dag_id == CANARY_DAG)  # Redundant, for performance.
            .all()
        )


def get_num_queued_tasks():
    """Number of queued tasks currently."""
    with session_scope(Session) as session:
        return (
            session.query(TaskInstance)
            .filter(TaskInstance.state == State.QUEUED)
            .count()
        )


def sla_check(sla_interval, sla_time, max_execution_date, cadence, execution_dates):
    utc_datetime = pytz.timezone("UTC").localize(datetime.datetime.utcnow())
    if sla_time:
        local_datetime = utc_datetime.astimezone(pytz.timezone(TIMEZONE_LA))
        sla_datetime = pytz.timezone(TIMEZONE_LA).localize(
            datetime.datetime.combine(
                local_datetime.date(),
                datetime.datetime.strptime(sla_time, "%H:%M").time()
            )
        )
    else:
        sla_datetime = utc_datetime

    interval_in_second = pytime_parse(sla_interval)
    checkpoint = sla_datetime - datetime.timedelta(seconds=interval_in_second)
    if utc_datetime >= sla_datetime:
        if max_execution_date < checkpoint:
            return True

        if cadence != "triggered":
            # Check the state of previous run before sla_time.
            # To detect consecutive failed scenario.
            # Filter out triggered DAGs e.g. PPD
            for record in execution_dates:
                if record["execution_date"] <= checkpoint:
                    return record["state"] != "success"

    return False


def get_sla_miss_dags():
    min_date_to_filter = pendulum.now(TIMEZONE).subtract(days=RETENTION_TIME)
    with session_scope(Session) as session:
        execution_dt_query = (
            session.query(
                DagRun.dag_id,
                DagModel.schedule_interval,
                DagRun.execution_date,
                DagRun.state,
            )
            .join(DagModel, DagModel.dag_id == DagRun.dag_id)
            .filter(
                DagModel.is_active == True,
                DagModel.is_paused == False,
                DagRun.execution_date > min_date_to_filter,
            )
            .order_by(DagRun.execution_date.desc())
            .subquery()
        )
        runs = (
            session.query(
                DelayAlertMetaData.affected_pipeline,
                DelayAlertMetaData.alert_external_classification,
                DelayAlertMetaData.alert_report_classification,
                DelayAlertMetaData.alert_target,
                DelayAlertMetaData.cadence,
                DelayAlertMetaData.dag_id,
                DelayAlertMetaData.group_business_line,
                DelayAlertMetaData.group_pagerduty,
                DelayAlertMetaData.inhibit_rule,
                DelayAlertMetaData.latest_successful_run,
                DelayAlertMetaData.link,
                DelayAlertMetaData.severity,
                DelayAlertMetaData.sla_interval,
                DelayAlertMetaData.sla_time,
                execution_dt_query.c.schedule_interval,
                execution_dt_query.c.execution_date,
                execution_dt_query.c.state,
            )
            .join(
                execution_dt_query,
                DelayAlertMetaData.dag_id == execution_dt_query.c.dag_id,
            )
            .filter(
                DelayAlertMetaData.sla_interval.isnot(None),
                DelayAlertMetaData.task_id.is_(None),
            )
            .all()
        )

        execution_dates = {}
        max_execution_dates = {}
        for run in runs:
            key = run.dag_id
            if key not in execution_dates:
                execution_dates[key] = []
            if key not in max_execution_dates and run.state == "success":
                max_execution_dates[key] = run.execution_date
            execution_dates[key].append(
                {
                    "execution_date": run.execution_date,
                    "state": run.state,
                }
            )

        sla_miss_metrics = []
        processed_runs = set()
        for run in runs:
            key = run.dag_id
            if key in processed_runs or key not in max_execution_dates:
                continue

            processed_runs.add(key)
            metrics = {
                "affected_pipeline": run.affected_pipeline or MISSING,
                "alert_external_classification": run.alert_external_classification
                or MISSING,
                "alert_report_classification": run.alert_report_classification
                or MISSING,
                "alert_target": run.alert_target or MISSING,
                "dag_id": run.dag_id,
                "group_business_line": run.group_business_line or MISSING,
                "group_pagerduty": run.group_pagerduty or MISSING,
                "inhibit_rule": run.inhibit_rule or MISSING,
                "link": run.link or MISSING,
                "severity": run.severity or MISSING,
            }
            max_execution_date = max_execution_dates[key]
            if (
                run.latest_successful_run is None
                or max_execution_date > run.latest_successful_run
            ):
                session.query(DelayAlertMetaData).filter(
                    DelayAlertMetaData.dag_id == run.dag_id
                ).update({DelayAlertMetaData.latest_successful_run: max_execution_date})
                session.commit()
            else:
                max_execution_date = run.latest_successful_run

            metrics["sla_miss"] = sla_check(
                run.sla_interval,
                run.sla_time,
                max_execution_date,
                run.cadence,
                execution_dates[key],
            )

            sla_miss_metrics.append(metrics)
        return sla_miss_metrics


def get_sla_miss_tasks():
    min_date_to_filter = pendulum.now(TIMEZONE).subtract(days=RETENTION_TIME)
    with session_scope(Session) as session:
        execution_dt_query = (
            session.query(
                DagModel.schedule_interval,
                TaskInstance.dag_id,
                TaskInstance.execution_date,
                TaskInstance.state,
                TaskInstance.task_id,
            )
            .join(DagModel, DagModel.dag_id == TaskInstance.dag_id)
            .filter(
                DagModel.is_active == True,
                DagModel.is_paused == False,
                TaskInstance.execution_date > min_date_to_filter,
            )
            .order_by(TaskInstance.execution_date.desc())
            .subquery()
        )
        runs = (
            session.query(
                DelayAlertMetaData.affected_pipeline,
                DelayAlertMetaData.alert_external_classification,
                DelayAlertMetaData.alert_report_classification,
                DelayAlertMetaData.alert_target,
                DelayAlertMetaData.cadence,
                DelayAlertMetaData.dag_id,
                DelayAlertMetaData.group_business_line,
                DelayAlertMetaData.group_pagerduty,
                DelayAlertMetaData.inhibit_rule,
                DelayAlertMetaData.latest_successful_run,
                DelayAlertMetaData.link,
                DelayAlertMetaData.severity,
                DelayAlertMetaData.sla_interval,
                DelayAlertMetaData.sla_time,
                DelayAlertMetaData.task_id,
                execution_dt_query.c.execution_date,
                execution_dt_query.c.schedule_interval,
                execution_dt_query.c.state,
            )
            .join(
                execution_dt_query,
                and_(
                    execution_dt_query.c.dag_id == DelayAlertMetaData.dag_id,
                    execution_dt_query.c.task_id == DelayAlertMetaData.task_id,
                ),
            )
            .filter(DelayAlertMetaData.sla_interval.isnot(None))
            .all()
        )
        execution_dates = {}
        max_execution_dates = {}
        for run in runs:
            key = (run.dag_id, run.task_id)
            if key not in execution_dates:
                execution_dates[key] = []
            if key not in max_execution_dates and run.state == "success":
                max_execution_dates[key] = run.execution_date
            execution_dates[key].append(
                {
                    "execution_date": run.execution_date,
                    "state": run.state,
                }
            )

        sla_miss_metrics = []
        processed_runs = set()
        for run in runs:
            key = (run.dag_id, run.task_id)
            if key in processed_runs or key not in max_execution_dates:
                continue

            processed_runs.add(key)
            metrics = {
                "affected_pipeline": run.affected_pipeline or MISSING,
                "alert_external_classification": run.alert_external_classification
                or MISSING,
                "alert_report_classification": run.alert_report_classification
                or MISSING,
                "alert_target": run.alert_target or MISSING,
                "dag_id": run.dag_id,
                "group_business_line": run.group_business_line or MISSING,
                "group_pagerduty": run.group_pagerduty or MISSING,
                "inhibit_rule": run.inhibit_rule or MISSING,
                "link": run.link or MISSING,
                "severity": run.severity or MISSING,
                "task_id": run.task_id or MISSING,
            }
            max_execution_date = max_execution_dates[key]
            if (
                run.latest_successful_run is None
                or max_execution_date > run.latest_successful_run
            ):
                session.query(DelayAlertMetaData).filter(
                    DelayAlertMetaData.dag_id == run.dag_id
                ).update({DelayAlertMetaData.latest_successful_run: max_execution_date})
                session.commit()
            else:
                max_execution_date = run.latest_successful_run

            metrics["sla_miss"] = sla_check(
                run.sla_interval,
                run.sla_time,
                max_execution_date,
                run.cadence,
                execution_dates[key],
            )

            sla_miss_metrics.append(metrics)
        return sla_miss_metrics


class MetricsCollector(object):
    """Metrics Collector for prometheus."""

    def describe(self):
        return []

    def collect(self):
        """Collect metrics."""
        # Task metrics
        task_info = get_task_state_info()
        t_state = GaugeMetricFamily(
            "airflow_task_status",
            "Shows the number of task instances with particular status",
            labels=[
                "dag_id",
                "task_id",
                "owner",
                "status",
                "cadence",
                "severity",
                "alert_target",
                "alert_external_classification",
                "alert_report_classification",
                "sla_time",
                "network",
                "business",
            ],
        )
        for task in task_info:
            t_state.add_metric(
                [
                    task.dag_id,
                    task.task_id,
                    task.owners,
                    task.state or MISSING,
                    task.cadence or MISSING,
                    task.severity or MISSING,
                    task.alert_target or MISSING,
                    task.alert_external_classification or MISSING,
                    task.alert_report_classification or MISSING,
                    task.sla_time or MISSING,
                    "network",
                    "business",
                ],
                task.value,
            )
        yield t_state

        task_duration = GaugeMetricFamily(
            "airflow_task_duration",
            "Duration of successful tasks in seconds",
            labels=["task_id", "dag_id", "execution_date"],
        )
        for task in get_task_duration_info():
            task_duration_value = (task.end_date - task.start_date).total_seconds()
            task_duration.add_metric(
                [
                    task.task_id,
                    task.dag_id,
                    task.execution_date.strftime("%Y-%m-%dT%H:%M%S"),
                ],
                task_duration_value,
            )
        yield task_duration

        task_failure_count = GaugeMetricFamily(
            "airflow_task_fail_count",
            "Count of failed tasks",
            labels=["dag_id", "task_id"],
        )
        for task in get_task_failure_counts():
            task_failure_count.add_metric([task.dag_id, task.task_id], task.count)
        yield task_failure_count

        # Dag Metrics
        dag_info = get_dag_state_info()
        d_state = GaugeMetricFamily(
            "airflow_dag_status",
            "Shows the number of dag starts with this status",
            labels=[
                "dag_id",
                "owner",
                "status",
                "cadence",
                "severity",
                "alert_target",
                "alert_external_classification",
                "alert_report_classification",
                "sla_time",
                "network",
                "business",
            ],
        )
        for dag in dag_info:
            d_state.add_metric(
                [
                    dag.dag_id,
                    dag.owners,
                    dag.state,
                    dag.cadence or MISSING,
                    dag.severity or MISSING,
                    dag.alert_target or MISSING,
                    dag.alert_external_classification or MISSING,
                    dag.alert_report_classification or MISSING,
                    dag.sla_time or MISSING,
                    "network",
                    "business",
                ],
                dag.count,
            )
        yield d_state

        dag_duration = GaugeMetricFamily(
            "airflow_dag_run_duration",
            "Duration of successful dag_runs in seconds",
            labels=["dag_id"],
        )
        for dag in get_dag_duration_info():
            dag_duration_value = (dag.end_date - dag.start_date).total_seconds()
            dag_duration.add_metric([dag.dag_id], dag_duration_value)
        yield dag_duration

        # Scheduler Metrics
        dag_scheduler_delay = GaugeMetricFamily(
            "airflow_dag_scheduler_delay",
            "Airflow DAG scheduling delay",
            labels=["dag_id"],
        )

        for dag in get_dag_scheduler_delay():
            dag_scheduling_delay_value = (
                dag.start_date - dag.execution_date
            ).total_seconds()
            dag_scheduler_delay.add_metric([dag.dag_id], dag_scheduling_delay_value)
        yield dag_scheduler_delay

        # XCOM parameters

        xcom_params = GaugeMetricFamily(
            "airflow_xcom_parameter",
            "Airflow Xcom Parameter",
            labels=["dag_id", "task_id"],
        )

        xcom_config = load_xcom_config()
        for tasks in xcom_config.get("xcom_params", []):
            for param in get_xcom_params(tasks["task_id"]):
                xcom_value = extract_xcom_parameter(param.value)

                if tasks["key"] in xcom_value:
                    xcom_params.add_metric(
                        [param.dag_id, param.task_id], xcom_value[tasks["key"]]
                    )

        yield xcom_params

        task_scheduler_delay = GaugeMetricFamily(
            "airflow_task_scheduler_delay",
            "Airflow Task scheduling delay",
            labels=["queue"],
        )

        for task in get_task_scheduler_delay():
            task_scheduling_delay_value = (
                task.start_date - task.queued_dttm
            ).total_seconds()
            task_scheduler_delay.add_metric([task.queue], task_scheduling_delay_value)
        yield task_scheduler_delay

        num_queued_tasks_metric = GaugeMetricFamily(
            "airflow_num_queued_tasks", "Airflow Number of Queued Tasks"
        )

        num_queued_tasks = get_num_queued_tasks()
        num_queued_tasks_metric.add_metric([], num_queued_tasks)
        yield num_queued_tasks_metric

        sla_miss_dags_metric = GaugeMetricFamily(
            "airflow_dags_sla_miss",
            "Airflow DAGS missing the sla",
            labels=[
                "affected_pipeline",
                "alert_external_classification",
                "alert_report_classification",
                "alert_target",
                "dag_id",
                "group_business_line",
                "group_pagerduty",
                "inhibit_rule",
                "link",
                "severity",
            ],
        )

        for dag in get_sla_miss_dags():
            sla_miss_dags_metric.add_metric(
                [
                    dag["affected_pipeline"],
                    dag["alert_external_classification"],
                    dag["alert_report_classification"],
                    dag["alert_target"],
                    dag["dag_id"],
                    dag["group_business_line"],
                    dag["group_pagerduty"],
                    dag["inhibit_rule"],
                    dag["link"],
                    dag["severity"],
                ],
                dag["sla_miss"],
            )

        yield sla_miss_dags_metric

        sla_miss_tasks_metric = GaugeMetricFamily(
            "airflow_tasks_sla_miss",
            "Airflow tasks missing the sla",
            labels=[
                "affected_pipeline",
                "alert_external_classification",
                "alert_report_classification",
                "alert_target",
                "dag_id",
                "group_business_line",
                "group_pagerduty",
                "inhibit_rule",
                "link",
                "severity",
                "task_id",
            ],
        )

        for tasks in get_sla_miss_tasks():
            sla_miss_tasks_metric.add_metric(
                [
                    tasks["affected_pipeline"],
                    tasks["alert_external_classification"],
                    tasks["alert_report_classification"],
                    tasks["alert_target"],
                    tasks["dag_id"],
                    tasks["group_business_line"],
                    tasks["group_pagerduty"],
                    tasks["inhibit_rule"],
                    tasks["link"],
                    tasks["severity"],
                    tasks["task_id"],
                ],
                tasks["sla_miss"],
            )
        yield sla_miss_tasks_metric


REGISTRY.register(MetricsCollector())

if RBAC:
    from flask_appbuilder import BaseView as FABBaseView, expose as FABexpose

    class RBACMetrics(FABBaseView):
        route_base = "/admin/metrics/"

        @FABexpose("/")
        def list(self):
            return Response(generate_latest(), mimetype="text")

    # Metrics View for Flask app builder used in airflow with rbac enabled
    RBACmetricsView = {"view": RBACMetrics(), "name": "Metrics", "category": "Public"}
    ADMIN_VIEW = []
    RBAC_VIEW = [RBACmetricsView]
else:

    class Metrics(BaseView):
        @expose("/")
        def index(self):
            return Response(generate_latest(), mimetype="text/plain")

    ADMIN_VIEW = [Metrics(category="Prometheus exporter", name="Metrics")]
    RBAC_VIEW = []


class AirflowPrometheusPlugin(AirflowPlugin):
    """Airflow Plugin for collecting metrics."""

    name = "airflow_prometheus_plugin"
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = ADMIN_VIEW
    flask_blueprints = []
    menu_links = []
    appbuilder_views = RBAC_VIEW
    appbuilder_menu_items = []
