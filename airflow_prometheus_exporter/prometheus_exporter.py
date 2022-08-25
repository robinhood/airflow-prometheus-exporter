"""Prometheus exporter for Airflow."""
import datetime
import time

import pygsheets
from contextlib import contextmanager
from flask import Response
from flask_appbuilder import BaseView, expose
from prometheus_client import REGISTRY, generate_latest
from prometheus_client.core import GaugeMetricFamily
from sqlalchemy import Boolean, Column, DateTime, String, Text, and_, func, types
from sqlalchemy.ext.declarative import declarative_base

from airflow.hooks.base import BaseHook
from airflow.models import DagModel, DagRun, TaskFail, TaskInstance, XCom
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
from airflow_prometheus_exporter.xcom_config import load_xcom_config

from .metrics import (
    MISSING,
    extract_xcom_parameter,
    get_dag_duration_info,
    get_dag_scheduler_delay,
    get_dag_state_info,
    get_latest_successful_dag_run,
    get_num_queued_tasks,
    get_sla_miss,
    get_task_duration_info,
    get_task_failure_counts,
    get_task_scheduler_delay,
    get_task_state_info,
    get_unmonitored_dag,
    get_xcom_params,
)


@contextmanager
def session_scope(session):
    """Provide a transactional scope around a series of operations."""
    try:
        yield session
    finally:
        session.close()


class UTCDateTime(types.TypeDecorator):
    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            if not value.tzinfo:
                raise TypeError("tzinfo is required")
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return value


with session_scope(Session) as session:
    Base = declarative_base(session.get_bind())

    class DelayAlertMetadata(Base):
        __tablename__ = "delay_alert_metadata"
        dag_id = Column(String(250), primary_key=True)
        task_id = Column(String(250), primary_key=True, nullable=True)
        sla_interval = Column(String(64), primary_key=True)
        sla_time = Column(String(5), primary_key=True, nullable=True)
        affected_pipeline = Column(Text, nullable=True)
        alert_name = Column(String(250), nullable=True)
        alert_target = Column(String(250), nullable=True)
        group_title = Column(Text, nullable=True)
        inhibit_rule = Column(Text, nullable=True)
        link = Column(Text, nullable=True)
        note = Column(Text, nullable=True)
        ready = Column(Boolean, nullable=True)

    class DelayAlertAuxiliaryInfo(Base):
        __tablename__ = "delay_alert_auxiliary_info"
        dag_id = Column(String(250), primary_key=True)
        task_id = Column(String(250), primary_key=True, nullable=True)
        sla_interval = Column(String(64), primary_key=True)
        sla_time = Column(String(5), primary_key=True, nullable=True)
        latest_successful_run = Column(UTCDateTime)
        latest_sla_miss_state = Column(Boolean)

    Base.metadata.create_all(checkfirst=True)


class MetricsCollector(object):
    """Metrics Collector for prometheus."""

    def describe(self):
        return []

    def collect(self):
        """Collect metrics."""

        start_time = time.monotonic()

        # Task metrics
        task_info = get_task_state_info(DagRun, TaskInstance, DagModel)
        t_state = GaugeMetricFamily(
            "airflow_task_status",
            "Shows the number of task instances with particular status",
            labels=[
                "dag_id",
                "task_id",
                "owner",
                "status",
            ],
        )
        for task in task_info:
            t_state.add_metric(
                [
                    task.dag_id,
                    task.task_id,
                    task.owners,
                    task.state or MISSING,
                ],
                task.value,
            )
        yield t_state

        task_duration = GaugeMetricFamily(
            "airflow_task_duration",
            "Duration of successful tasks in seconds",
            labels=["task_id", "dag_id", "execution_date"],
        )
        for task in get_task_duration_info(DagModel, DagRun, TaskInstance):
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
        for task in get_task_failure_counts(TaskFail, DagModel):
            task_failure_count.add_metric([task.dag_id, task.task_id], task.count)
        yield task_failure_count

        # Dag Metrics
        dag_info = get_dag_state_info(DagRun, DagModel)
        d_state = GaugeMetricFamily(
            "airflow_dag_status",
            "Shows the number of dag starts with this status",
            labels=[
                "dag_id",
                "owner",
                "status",
            ],
        )
        for dag in dag_info:
            d_state.add_metric(
                [
                    dag.dag_id,
                    dag.owners,
                    dag.state,
                ],
                dag.count,
            )
        yield d_state

        dag_duration = GaugeMetricFamily(
            "airflow_dag_run_duration",
            "Duration of successful dag_runs in seconds",
            labels=["dag_id"],
        )
        for dag in get_dag_duration_info(DagRun, DagModel, TaskInstance):
            dag_duration_value = (dag.end_date - dag.start_date).total_seconds()
            dag_duration.add_metric([dag.dag_id], dag_duration_value)
        yield dag_duration

        # Scheduler Metrics
        dag_scheduler_delay = GaugeMetricFamily(
            "airflow_dag_scheduler_delay",
            "Airflow DAG scheduling delay",
            labels=["dag_id"],
        )

        for dag in get_dag_scheduler_delay(DagRun):
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
            for param in get_xcom_params(TaskInstance, DagRun, XCom, tasks["task_id"]):
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

        for task in get_task_scheduler_delay(DagRun, TaskInstance):
            task_scheduling_delay_value = (
                task.start_date - task.queued_dttm
            ).total_seconds()
            task_scheduler_delay.add_metric([task.queue], task_scheduling_delay_value)
        yield task_scheduler_delay

        num_queued_tasks_metric = GaugeMetricFamily(
            "airflow_num_queued_tasks", "Airflow Number of Queued Tasks"
        )

        num_queued_tasks = get_num_queued_tasks(TaskInstance)
        num_queued_tasks_metric.add_metric([], num_queued_tasks)
        yield num_queued_tasks_metric

        sla_miss_metric = GaugeMetricFamily(
            "airflow_sla_miss",
            "Airflow DAGS missing the sla",
            labels=[
                "dag_id",
                "task_id",
                "affected_pipeline",
                "alert_name",
                "alert_target",
                "group_title",
                "inhibit_rule",
                "link",
                "sla_interval",
                "sla_time",
            ],
        )

        for alert in get_sla_miss(
            DelayAlertMetadata, DelayAlertAuxiliaryInfo, DagModel, DagRun, TaskInstance
        ):
            sla_miss_metric.add_metric(
                [
                    alert["dag_id"],
                    alert["task_id"],
                    alert["affected_pipeline"],
                    alert["alert_name"],
                    alert["alert_target"],
                    alert["group_title"],
                    alert["inhibit_rule"],
                    alert["link"],
                    alert["sla_interval"],
                    alert["sla_time"],
                ],
                alert["sla_miss"],
            )

        yield sla_miss_metric

        unmonitored_dag_metric = GaugeMetricFamily(
            "airflow_unmonitored_dag",
            "Airflow Unmonitored DAG",
            labels=["dag_id"],
        )

        for r in get_unmonitored_dag(DagModel, DelayAlertMetadata):
            unmonitored_dag_metric.add_metric([r.dag_id], True)
        yield unmonitored_dag_metric

        extraction_time = GaugeMetricFamily(
            "exporter_extraction_duration",
            "Duration of exporter extraction in seconds",
            labels=["elapsed_time"],
        )

        extraction_time.add_metric(["elapsed_time"], time.monotonic() - start_time)
        yield extraction_time


REGISTRY.register(MetricsCollector())


class RBACMetrics(BaseView):
    route_base = "/admin/metrics/"

    @expose("/list/")
    def list(self):
        return Response(generate_latest(), mimetype="text")

    @expose("/ddns/dag_run/")
    def dag_run(self):
        return Response(
            get_latest_successful_dag_run(DagModel, DagRun, column_name=True),
            mimetype="text",
        )

    @expose("/ddns/task_instance/")
    def task_instance(self):
        return Response("Hey, im here", mimetype="text")


# Metrics View for Flask app builder used in airflow with rbac enabled
RBACmetricsView = {"view": RBACMetrics(), "name": "Metrics", "category": "Public"}
ADMIN_VIEW = []
RBAC_VIEW = [RBACmetricsView]


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
