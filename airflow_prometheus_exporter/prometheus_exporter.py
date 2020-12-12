"""Prometheus exporter for Airflow."""
import json
import os
import pickle
from contextlib import contextmanager
from datetime import datetime

import croniter
import dateparser
import pendulum
from flask import Response
from flask_admin import BaseView, expose
from prometheus_client import REGISTRY, generate_latest
from prometheus_client.core import GaugeMetricFamily
from sqlalchemy import Column, String, and_, func
from sqlalchemy.ext.declarative import declarative_base

from airflow.configuration import conf
from airflow.models import DagModel, DagRun, TaskFail, TaskInstance, XCom
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import RBAC, Session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from airflow_prometheus_exporter.xcom_config import load_xcom_config

CANARY_DAG = "canary_dag"
RETENTION_TIME = os.environ.get("PROMETHEUS_METRICS_DAYS", 14)
TIMEZONE = conf.get("core", "default_timezone")


@contextmanager
def session_scope(session):
    """Provide a transactional scope around a series of operations."""
    try:
        yield session
    finally:
        session.close()


with session_scope(Session) as session:
    Base = declarative_base(session.get_bind())

    class GapDagTag(Base):
        __tablename__ = "gap_dag_tag"
        dag_id = Column(String, primary_key=True)  # hack to have dag_id as PK
        __table_args__ = {"autoload": True}


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
                GapDagTag.cadence,
                GapDagTag.severerity,
                GapDagTag.alert_target,
                GapDagTag.instant_slack_alert,
                GapDagTag.alert_classification,
                GapDagTag.sla_time,
            )
            .join(DagModel, DagModel.dag_id == dag_status_query.c.dag_id)
            .filter(DagModel.is_active == True, DagModel.is_paused == False)  # noqa
            .outerjoin(GapDagTag, GapDagTag.dag_id == dag_status_query.c.dag_id)
            .filter(GapDagTag.task_id.is_(None))
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
                GapDagTag.cadence,
                GapDagTag.severerity,
                GapDagTag.alert_target,
                GapDagTag.instant_slack_alert,
                GapDagTag.alert_classification,
                GapDagTag.sla_time,
            )
            .join(DagModel, DagModel.dag_id == task_status_query.c.dag_id)
            .filter(DagModel.is_active == True, DagModel.is_paused == False)  # noqa
            .outerjoin(
                GapDagTag,
                (GapDagTag.dag_id == task_status_query.c.dag_id)
                & (GapDagTag.task_id == task_status_query.c.task_id),  # noqa
            )
            .filter(GapDagTag.task_id.isnot(None))
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


def get_sla_miss_dags():
    min_date_to_filter = pendulum.now(TIMEZONE).subtract(days=RETENTION_TIME)
    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id,
                DagModel.schedule_interval,
                func.max(DagRun.execution_date).label("max_execution_date"),
            )
            .join(DagModel, DagModel.dag_id == DagRun.dag_id)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
                DagRun.state == State.SUCCESS,
                DagRun.execution_date > min_date_to_filter,
            )
            .group_by(DagRun.dag_id, DagModel.schedule_interval)
            .subquery()
        )
        dags = (
            session.query(
                GapDagTag.dag_id,
                GapDagTag.sla_interval,
                GapDagTag.sla_time,
                GapDagTag.alert_target,
                GapDagTag.alert_classification,
                max_execution_dt_query.c.schedule_interval,
                max_execution_dt_query.c.max_execution_date,
            )
            .join(
                max_execution_dt_query,
                GapDagTag.dag_id == max_execution_dt_query.c.dag_id,
            )
            .filter(GapDagTag.sla_interval.isnot(None), GapDagTag.task_id.is_(None))
            .all()
        )
        sla_miss_dags_metrics = []
        for dag in dags:
            dag_metrics = {
                "dag_id": dag.dag_id,
                "alert_target": dag.alert_target,
                "alert_classification": dag.alert_classification,
                "sla_miss": 0,
            }
            try:
                cron = croniter.croniter(dag.schedule_interval)
                expected_last_run = cron.get_prev(datetime)
                diff_from_expected = (
                    pendulum.instance(expected_last_run)
                    - pendulum.instance(dag.max_execution_date)
                ).in_minutes()
                sla_time = dateparser.parse(
                    "today " + dag.sla_time,
                    settings={"TIMEZONE": "America/Los_Angeles"},
                )
                if pendulum.now(
                    "America/Los_Angeles"
                ) > sla_time and diff_from_expected > (dag.sla_interval * 24 * 60):
                    dag_metrics["sla_miss"] = 1
            except:
                pass
            sla_miss_dags_metrics.append(dag_metrics)
        return sla_miss_dags_metrics


def get_sla_miss_tasks():
    min_date_to_filter = pendulum.now(TIMEZONE).subtract(days=RETENTION_TIME)
    with session_scope(Session) as session:
        max_execution_date_query = (
            session.query(
                TaskInstance.dag_id,
                TaskInstance.task_id,
                DagModel.schedule_interval,
                func.max(TaskInstance.execution_date).label("max_execution_date"),
            )
            .join(DagModel, DagModel.dag_id == TaskInstance.dag_id)
            .filter(
                DagModel.is_active == True,
                DagModel.is_paused == False,
                TaskInstance.state == State.SUCCESS,
                TaskInstance.execution_date > min_date_to_filter,
            )
            .group_by(
                TaskInstance.dag_id, TaskInstance.task_id, DagModel.schedule_interval
            )
            .subquery()
        )

        tasks = (
            session.query(
                GapDagTag.dag_id,
                GapDagTag.task_id,
                max_execution_date_query.c.max_execution_date,
                max_execution_date_query.c.schedule_interval,
                GapDagTag.sla_interval,
                GapDagTag.sla_time,
                GapDagTag.alert_target,
                GapDagTag.alert_classification,
            )
            .join(
                max_execution_date_query,
                and_(
                    max_execution_date_query.c.dag_id == GapDagTag.dag_id,
                    max_execution_date_query.c.task_id == GapDagTag.task_id,
                ),
            )
            .filter(GapDagTag.sla_interval.isnot(None))
            .all()
        )
        sla_miss_tasks = []
        for task in tasks:
            task_metrics = {
                "dag_id": task.dag_id,
                "task_id": task.task_id,
                "alert_target": task.alert_target,
                "alert_classification": task.alert_classification,
                "sla_miss": 0,
            }
            try:
                cron = croniter.croniter(task.schedule_interval)
                expected_last_run = cron.get_prev(datetime)
                diff_from_expected = (
                    pendulum.instance(expected_last_run)
                    - pendulum.instance(dag.max_execution_date)
                ).in_minutes()
                sla_time = dateparser.parse(
                    "today " + task.sla_time,
                    settings={"TIMEZONE": "America/Los_Angeles"},
                )
                if pendulum.now(
                    "America/Los_Angeles"
                ) > sla_time and diff_from_expected > (task.sla_interval * 24 * 60):
                    task["sla_miss"] = 1
            except:
                pass
            sla_miss_tasks.append(task_metrics)
        return sla_miss_tasks


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
                "instant_slack_alert",
                "alert_classification",
                "sla_time",
            ],
        )
        for task in task_info:
            t_state.add_metric(
                [
                    task.dag_id,
                    task.task_id,
                    task.owners,
                    task.state or "none",
                    task.cadence or "none",
                    task.severerity or "none",
                    task.alert_target or "none",
                    task.instant_slack_alert or "none",
                    task.alert_classification or "none",
                    task.sla_time or "none",
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
                "instant_slack_alert",
                "alert_classification",
                "sla_time",
            ],
        )
        for dag in dag_info:
            d_state.add_metric(
                [
                    dag.dag_id,
                    dag.owners,
                    dag.state,
                    dag.cadence or "none",
                    dag.severerity or "none",
                    dag.alert_target or "none",
                    dag.instant_slack_alert or "none",
                    dag.alert_classification or "none",
                    dag.sla_time or "none",
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
            labels=["dag_id", "alert_target", "alert_classification"],
        )

        for dag in get_sla_miss_dags():
            sla_miss_dags_metric.add_metric(
                [dag["dag_id"], dag["alert_target"], dag["alert_classification"]],
                dag["sla_miss"],
            )

        yield sla_miss_dags_metric

        sla_miss_tasks_metric = GaugeMetricFamily(
            "airflow_tasks_sla_miss",
            "Airflow tasks missing the sla",
            labels=["dag_id", "task_id", "alert_target", "alert_classification"],
        )

        for tasks in get_sla_miss_tasks():
            sla_miss_tasks_metric.add_metric(
                [
                    tasks["dag_id"],
                    tasks["task_id"],
                    tasks["alert_target"],
                    tasks["alert_classification"],
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
