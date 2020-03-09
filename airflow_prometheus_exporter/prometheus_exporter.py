"""Prometheus exporter for Airflow"""

import json
import pickle
import logging
import datetime as dt
from functools import wraps
from contextlib import contextmanager

import pytz
from croniter import croniter
from sqlalchemy import and_, func
from flask import request, Response
from flask_admin import BaseView, expose
from prometheus_client.core import GaugeMetricFamily
from prometheus_client import generate_latest, REGISTRY

from airflow.utils.state import State
from airflow.configuration import conf
from airflow.settings import RBAC, Session
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import DagModel, DagRun, TaskInstance, TaskFail


@contextmanager
def session_scope(session):
    """Provide a transactional scope around a series of operations"""
    try:
        yield session
    finally:
        session.close()


def get_dag_states():
    """Number of dag run states for the current hour"""

    hour_now = dt.datetime.now().replace(
        minute=0, second=0, microsecond=0, tzinfo=pytz.UTC
    )
    hour_later = hour_now + dt.timedelta(hours=1)

    with session_scope(Session) as session:
        dag_status_query = (
            session.query(
                DagRun.dag_id, DagRun.state, func.count(DagRun.state).label("count"),
            )
            .filter(DagRun.execution_date.between(hour_now, hour_later))
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
            .filter(DagModel.is_active == True, DagModel.is_paused == False,)  # noqa
            .all()
        )


def get_dag_run_durations():
    """Duration of successful dag runs in seconds"""

    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("max_execution_dt"),
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


def get_dag_schedule_delays():
    """Schedule delay for dags in seconds"""

    now = dt.datetime.now().replace(tzinfo=pytz.UTC)
    week_ago = now - dt.timedelta(weeks=1)

    with session_scope(Session) as session:
        max_id_query = (
            session.query(func.max(DagRun.id))
            .filter(DagRun.execution_date.between(week_ago, now))
            .group_by(DagRun.dag_id)
            .subquery()
        )

        return (
            session.query(
                DagModel.dag_id,
                DagModel.schedule_interval,
                DagRun.execution_date,
                DagRun.start_date,
            )
            .join(DagModel, DagModel.dag_id == DagRun.dag_id)
            .filter(
                DagModel.is_active == True,
                DagModel.is_paused == False,
                DagModel.schedule_interval.isnot(None),
                DagRun.id.in_(max_id_query),
            )
            .all()
        )


class MetricsCollector(object):
    """Metrics collector for prometheus"""

    def describe(self):
        return []

    def collect(self):
        """Collect metrics"""

        airflow_dag_status = GaugeMetricFamily(
            "airflow_dag_status",
            "Number of dag run states for the current hour",
            labels=["dag_id", "owner", "status"],
        )
        for dag in get_dag_states():
            airflow_dag_status.add_metric(
                [dag.dag_id, dag.owners, dag.state], dag.count
            )
        yield airflow_dag_status

        airflow_dag_run_duration = GaugeMetricFamily(
            "airflow_dag_run_duration",
            "Duration of successful dag runs in seconds",
            labels=["dag_id"],
        )
        for dag in get_dag_run_durations():
            try:
                dag_run_duration = (dag.end_date - dag.start_date).total_seconds()
                airflow_dag_run_duration.add_metric([dag.dag_id], dag_run_duration)
            except TypeError:
                pass
        yield airflow_dag_run_duration

        airflow_dag_schedule_delay = GaugeMetricFamily(
            "airflow_dag_schedule_delay",
            "Airflow dag schedule delay in seconds",
            labels=["dag_id"],
        )
        for dag in get_dag_schedule_delays():
            c = croniter(dag.schedule_interval, dag.execution_date)
            planned_start_date = c.get_next(dt.datetime)

            dag_schedule_delay = (dag.start_date - planned_start_date).total_seconds()
            airflow_dag_schedule_delay.add_metric([dag.dag_id], dag_schedule_delay)
        yield airflow_dag_schedule_delay


REGISTRY.register(MetricsCollector())


def valid_token(request_token):
    api_token = conf.get("user", "api_token")
    return api_token == request_token


def has_access(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        authorization = request.headers.get("Authorization", None)
        if authorization is None:
            return Response("Bad request!", 400)

        request_token = authorization.split(" ")[1]
        if not request_token or not valid_token(request_token):
            return Response("Invalid token!", 401)
        return f(*args, **kwargs)

    return wrapper


if RBAC:
    from flask_appbuilder import (
        BaseView as FABBaseView,
        expose as FABexpose,
    )

    class RBACMetrics(FABBaseView):
        route_base = "/admin/metrics/"

        @FABexpose("/")
        @has_access
        def list(self):
            return Response(generate_latest(), mimetype="text")

    # Metrics View for Flask app builder used in airflow with rbac enabled
    RBACmetricsView = {"view": RBACMetrics(), "name": "Metrics", "category": "Public"}
    ADMIN_VIEW = []
    RBAC_VIEW = [RBACmetricsView]
else:

    class BaseMetrics(BaseView):
        @expose("/")
        def index(self):
            return Response(generate_latest(), mimetype="text/plain")

    ADMIN_VIEW = [
        BaseMetrics(category="Prometheus exporter", name="Metrics", endpoint="metrics")
    ]
    RBAC_VIEW = []


class AirflowPrometheusPlugin(AirflowPlugin):
    """Airflow plugin for collecting metrics"""

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
