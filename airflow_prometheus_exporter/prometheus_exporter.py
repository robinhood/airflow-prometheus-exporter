"""Prometheus exporter for Airflow."""
import datetime
import json
import os
import pickle
import pytz
from collections import defaultdict
from contextlib import contextmanager

import dateparser
import pendulum
from flask import Response
from flask_appbuilder import BaseView, expose
from prometheus_client import REGISTRY, generate_latest
from prometheus_client.core import GaugeMetricFamily
from pytimeparse import parse as pytime_parse
from sqlalchemy import Column, String, and_, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.expression import null
from sqlalchemy_utcdatetime import UTCDateTime

from airflow.configuration import conf
from airflow.models import DagModel, DagRun, TaskFail, TaskInstance, XCom
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from airflow_prometheus_exporter.xcom_config import load_xcom_config

CANARY_DAG = "canary_dag"
RETENTION_TIME = os.environ.get("PROMETHEUS_METRICS_DAYS", 21)
TIMEZONE = conf.get("core", "default_timezone")
TIMEZONE_LA = "America/Los_Angeles"
MISSING = "missing"
DEFAULT_LINK = "https://openmail.atlassian.net/wiki/spaces/ETL/pages/2132508806/Help"


@contextmanager
def session_scope(session):
    """Provide a transactional scope around a series of operations."""
    try:
        yield session
    finally:
        session.close()


with session_scope(Session) as session:
    Base = declarative_base(session.get_bind())

    class DelayAlertMetadata(Base):
        __tablename__ = "ddns_delay_alert_metadata"
        __table_args__ = {"autoload": True}
        dag_id = Column(String, primary_key=True)


    class DelayAlertAuxiliaryInfo(Base):
        __tablename__ = "ddns_delay_alert_auxiliary_info"
        __table_args__ = {"autoload": True}
        dag_id = Column(String, primary_key=True)
        task_id = Column(String, primary_key=True)
        latest_successful_run = Column(UTCDateTime)


######################
# Task Related Metrics
######################

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

def sla_check(sla_interval, sla_time, max_execution_date, cadence, latest_sla_miss_state):
    utc_datetime = pytz.timezone(TIMEZONE).localize(datetime.datetime.utcnow())

    if sla_time:
        # Convert user defined SLA time to local datetime.
        local_datetime = utc_datetime.astimezone(pytz.timezone(TIMEZONE_LA))
        sla_datetime = pytz.timezone(TIMEZONE_LA).localize(
            datetime.datetime.combine(
                local_datetime.date(),
                datetime.datetime.strptime(sla_time, "%H:%M").time(),
            )
        )
    else:
        # If no defined SLA time, meaning we check SLA miss every time.
        sla_datetime = utc_datetime

    interval_in_second = pytime_parse(sla_interval)
    checkpoint = sla_datetime - datetime.timedelta(seconds=interval_in_second)

    # Check SLA miss when it's SLA time.
    if utc_datetime >= sla_datetime:
        return max_execution_date < checkpoint

    # Check SLA miss when it's before SLA time to see the state of previous run.
    if cadence != "triggered":
        # Default to False in case of new alert before SLA time
        return latest_sla_miss_state or False

    return False


def upsert_auxiliary_info(session, upsert_dict):
    for k, v in upsert_dict.items():
        dag_id, task_id = k
        value = v["value"]
        insert = v["insert"]
        latest_successful_run = value["max_execution_date"]
        latest_sla_miss_state = value["sla_miss"]

        if insert:
            session.add(DelayAlertAuxiliaryInfo(
                dag_id=dag_id,
                task_id=task_id,
                latest_successful_run=latest_successful_run,
                latest_sla_miss_state=latest_sla_miss_state,
            ))
        else:
            session.query(DelayAlertAuxiliaryInfo).filter(
                DelayAlertAuxiliaryInfo.dag_id == dag_id,
                DelayAlertAuxiliaryInfo.task_id == task_id,
            ).update({
                DelayAlertAuxiliaryInfo.latest_successful_run: latest_successful_run,
                DelayAlertAuxiliaryInfo.latest_sla_miss_state: latest_sla_miss_state,
            })
    session.flush()
    session.commit()


def get_sla_miss():
    with session_scope(Session) as session:
        active_alert_query = (
            session.query(
                DelayAlertMetadata.dag_id,
                DelayAlertMetadata.task_id,
            )
            .join(
                DagModel,
                DelayAlertMetadata.dag_id == DagModel.dag_id
            )
            .filter(
                DagModel.is_active == True,
                DagModel.is_paused == False,
            )
            .group_by(
                DelayAlertMetadata.dag_id,
                DelayAlertMetadata.task_id,
            )
            .subquery()
        )

        # Gather the current max execution dates
        dag_max_execution_date = (
            session.query(
                DagRun.dag_id,
                null().label("task_id"),
                func.max(DagRun.execution_date).label("execution_date"),
            )
            .join(
                active_alert_query,
                (DagRun.dag_id == active_alert_query.c.dag_id)
                & (active_alert_query.c.task_id.is_(None)),
            )
            .filter(
                DagRun.state == State.SUCCESS,
                DagRun.end_date.isnot(None),
            )
            .group_by(DagRun.dag_id)
        )

        task_max_execution_date = (
            session.query(
                TaskInstance.dag_id,
                TaskInstance.task_id,
                func.max(TaskInstance.execution_date).label("execution_date"),
            )
            .join(
                active_alert_query,
                (TaskInstance.dag_id == active_alert_query.c.dag_id)
                & (TaskInstance.task_id == active_alert_query.c.task_id),
            )
            .filter(
                TaskInstance.state == State.SUCCESS,
                TaskInstance.end_date.isnot(None),
            )
            .group_by(
                TaskInstance.dag_id,
                TaskInstance.task_id,
            )
            .union(
                dag_max_execution_date
            )
        )

        max_execution_dates = {}
        for r in dag_max_execution_date:
            max_execution_dates[(r.dag_id, r.task_id)] = r.execution_date

        # Getting all alerts with auxiliary data
        alert_query = (
            session.query(
                DelayAlertMetadata.dag_id,
                DelayAlertMetadata.task_id,
                DelayAlertMetadata.affected_pipeline, DelayAlertMetadata.alert_target,
                DelayAlertMetadata.alert_name,
                DelayAlertMetadata.cadence,
                DelayAlertMetadata.group_title,
                DelayAlertMetadata.inhibit_rule,
                DelayAlertMetadata.link,
                DelayAlertMetadata.sla_interval,
                DelayAlertMetadata.sla_time,
                DelayAlertAuxiliaryInfo.latest_successful_run,
                DelayAlertAuxiliaryInfo.latest_sla_miss_state,
            )
            .join(
                active_alert_query,
                and_(
                    DelayAlertMetadata.dag_id == active_alert_query.c.dag_id,
                    func.coalesce( DelayAlertMetadata.task_id, "n/a")
                    == func.coalesce(active_alert_query.c.task_id, "n/a")
                ),
            )
            .join(
                DelayAlertAuxiliaryInfo,
                and_(
                    DelayAlertMetadata.dag_id == DelayAlertAuxiliaryInfo.dag_id,
                    func.coalesce(DelayAlertMetadata.task_id, "n/a")
                    == func.coalesce(DelayAlertAuxiliaryInfo.task_id, "n/a")
                ),
                isouter=True
            )
        )

        epoch = pytz.timezone(TIMEZONE).localize(datetime.datetime.utcfromtimestamp(0))
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
                alert.cadence,
                alert.latest_sla_miss_state,
            )

            if insert or update or sla_miss != alert.latest_sla_miss_state:
                upsert_dict[key] = {
                    "value":  {
                        "max_execution_date": max_execution_date,
                        "sla_miss": sla_miss,
                    },
                    "insert": insert,
                }

            yield {
                "dag_id": alert.dag_id,
                "task_id": alert.task_id or MISSING,
                "affected_pipeline": alert.affected_pipeline or MISSING,
                "alert_name": alert.alert_name or alert.dag_id,
                "alert_target": alert.alert_target or MISSING,
                "group_title": alert.group_title or alert.alert_name,
                "inhibit_rule": alert.inhibit_rule or MISSING,
                "link": alert.link or DEFAULT_LINK,
                "sla_interval": alert.sla_interval,
                "sla_miss": sla_miss,
                "sla_time": alert.sla_time or MISSING,
            }

        upsert_auxiliary_info(session, upsert_dict)


def get_unmonitored_dag():
    with session_scope(Session) as session:
        query = (
            session.query(
                DagModel.dag_id
            )
            .join(
                DelayAlertMetadata,
                DagModel.dag_id == DelayAlertMetadata.dag_id,
                isouter=True,
            )
            .filter(
                DelayAlertMetadata.dag_id.is_(None),
                DagModel.is_active == True,
                DagModel.is_paused == False,
            )
        )

        for r in query:
            yield r.dag_id


class MetricsCollector(object):
    """Metrics Collector for prometheus."""

    def describe(self):
        return []

    def collect(self):
        """Collect metrics."""
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

        for alert in get_sla_miss():
            sla_miss_dags_metric.add_metric(
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

        for dag_id in get_unmonitored_dag():
            unmonitored_dag_metric.add_metric([dag_id], True)
        yield unmonitored_dag_metric


REGISTRY.register(MetricsCollector())


class RBACMetrics(BaseView):
    route_base = "/admin/metrics/"

    @expose("/")
    def list(self):
        return Response(generate_latest(), mimetype="text")


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
