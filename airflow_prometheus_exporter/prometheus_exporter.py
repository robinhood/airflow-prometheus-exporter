"""Prometheus exporter for Airflow."""

import json
import pickle
import logging
from functools import wraps
from contextlib import contextmanager

from flask import request, Response
from sqlalchemy import and_, func
from flask_admin import BaseView, expose
from prometheus_client.core import GaugeMetricFamily
from prometheus_client import generate_latest, REGISTRY

from airflow.utils.state import State
from airflow.configuration import conf
from airflow.settings import RBAC, Session
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import DagModel, DagRun, TaskInstance, TaskFail


CANARY_DAG = "canary_dag"


@contextmanager
def session_scope(session):
    """Provide a transactional scope around a series of operations"""
    try:
        yield session
    finally:
        session.close()


#####################
# DAG Related Metrics
#####################


def get_dag_state_info():
    """Number of DAG Runs with particular state"""
    with session_scope(Session) as session:
        dag_status_query = (
            session.query(
                DagRun.dag_id, DagRun.state, func.count(DagRun.state).label("count"),
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
            .filter(DagModel.is_active == True, DagModel.is_paused == False,)  # noqa
            .all()
        )


def get_dag_duration_info():
    """Duration of successful DAG runs"""
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


######################
# Task Related Metrics
######################


def get_task_state_info():
    """Number of task instances with particular state"""
    with session_scope(Session) as session:
        task_status_query = (
            session.query(
                TaskInstance.dag_id,
                TaskInstance.task_id,
                TaskInstance.state,
                func.count(TaskInstance.dag_id).label("value"),
            )
            .group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state)
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
            .filter(DagModel.is_active == True, DagModel.is_paused == False,)  # noqa
            .all()
        )


def get_task_failure_counts():
    """Compute task failure counts"""
    with session_scope(Session) as session:
        return (
            session.query(
                TaskFail.dag_id,
                TaskFail.task_id,
                func.count(TaskFail.dag_id).label("count"),
            )
            .join(DagModel, DagModel.dag_id == TaskFail.dag_id,)
            .filter(DagModel.is_active == True, DagModel.is_paused == False,)  # noqa
            .group_by(TaskFail.dag_id, TaskFail.task_id,)
        )


def get_task_duration_info():
    """Duration of successful tasks in seconds"""
    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("max_execution_dt"),
            )
            .join(DagModel, DagModel.dag_id == DagRun.dag_id,)
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


###########################
# Scheduler Related Metrics
###########################


def get_dag_scheduler_delay():
    """Compute DAG scheduling delay"""
    with session_scope(Session) as session:
        return (
            session.query(DagRun.dag_id, DagRun.execution_date, DagRun.start_date,)
            .filter(DagRun.dag_id == CANARY_DAG,)
            .order_by(DagRun.execution_date.desc())
            .limit(1)
            .all()
        )


def get_task_scheduler_delay():
    """Compute task scheduling delay"""
    with session_scope(Session) as session:
        task_status_query = (
            session.query(
                TaskInstance.queue,
                func.max(TaskInstance.start_date).label("max_start"),
            )
            .filter(
                TaskInstance.dag_id == CANARY_DAG, TaskInstance.queued_dttm.isnot(None),
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
            .filter(TaskInstance.dag_id == CANARY_DAG,)  # Redundant, for performance.
            .all()
        )


def get_num_queued_tasks():
    """Number of queued tasks currently"""
    with session_scope(Session) as session:
        return (
            session.query(TaskInstance)
            .filter(TaskInstance.state == State.QUEUED)
            .count()
        )


class MetricsCollector(object):
    """Metrics collector for prometheus"""

    def describe(self):
        return []

    def collect(self):
        """Collect metrics."""

        # Task metrics
        task_info = get_task_state_info()
        t_state = GaugeMetricFamily(
            "airflow_task_status",
            "Shows the number of task instances with particular status",
            labels=["dag_id", "task_id", "owner", "status"],
        )
        for task in task_info:
            t_state.add_metric(
                [task.dag_id, task.task_id, task.owners, task.state or "none"],
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
                [task.task_id, task.dag_id, str(task.execution_date.date())],
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

        # DAG metrics
        dag_info = get_dag_state_info()
        d_state = GaugeMetricFamily(
            "airflow_dag_status",
            "Shows the number of dag starts with this status",
            labels=["dag_id", "owner", "status"],
        )
        for dag in dag_info:
            d_state.add_metric([dag.dag_id, dag.owners, dag.state], dag.count)
        yield d_state

        dag_duration = GaugeMetricFamily(
            "airflow_dag_run_duration",
            "Duration of successful DAG runs in seconds",
            labels=["dag_id"],
        )
        for dag in get_dag_duration_info():
            try:
                dag_duration_value = (dag.end_date - dag.start_date).total_seconds()
                dag_duration.add_metric([dag.dag_id], dag_duration_value)
            except TypeError:
                pass
        yield dag_duration

        # Scheduler metrics
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

        task_scheduler_delay = GaugeMetricFamily(
            "airflow_task_scheduler_delay",
            "Airflow task scheduling delay",
            labels=["queue"],
        )

        for task in get_task_scheduler_delay():
            task_scheduling_delay_value = (
                task.start_date - task.queued_dttm
            ).total_seconds()
            task_scheduler_delay.add_metric([task.queue], task_scheduling_delay_value)
        yield task_scheduler_delay

        num_queued_tasks_metric = GaugeMetricFamily(
            "airflow_num_queued_tasks", "Airflow number of queued tasks",
        )

        num_queued_tasks = get_num_queued_tasks()
        num_queued_tasks_metric.add_metric([], num_queued_tasks)
        yield num_queued_tasks_metric


REGISTRY.register(MetricsCollector())


def valid_token(request_token):
    auth_token = conf.get("user", "auth_token")
    return auth_token == request_token


def has_access(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        authorization = request.headers["Authorization"]
        request_token = authorization.replace("token ", "")
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
        @has_access
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
