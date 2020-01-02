# Airflow Prometheus Exporter

[![Build Status](https://travis-ci.org/robinhood/airflow-prometheus-exporter.svg?branch=master)](https://travis-ci.org/robinhood/airflow-prometheus-exporter)

The Airflow Prometheus Exporter exposes various metrics about the Scheduler, DAGs and Tasks which helps improve the observability of an Airflow cluster.

The exporter is based on this [prometheus exporter for Airflow](https://github.com/epoch8/airflow-exporter).

## Requirements

The plugin has been tested with:

- Airflow >= 1.10.4
- Python 3.6+

The scheduler metrics assume that there is a DAG named `canary_dag`. In our setup, the `canary_dag` is a DAG which has a tasks which perform very simple actions such as establishing database connections. This DAG is used to test the uptime of the Airflow scheduler itself.

## Installation

The exporter can be installed as an Airflow Plugin using:

```pip install airflow-prometheus-exporter```

This should ideally be installed in your Airflow virtualenv.

## Metrics

Metrics will be available at

`http://<your_airflow_host_and_port>/admin/metrics/`

### Task Specific Metrics

#### `airflow_task_status`

Number of tasks with a specific status.

All the possible states are listed [here](https://github.com/apache/airflow/blob/master/airflow/utils/state.py#L46).

#### `airflow_task_duration`

Duration of successful tasks in seconds.

#### `airflow_task_fail_count`

Number of times a particular task has failed.

#### `airflow_xcom_param`

value of configurable parameter in xcom table

xcom fields is deserialized as a dictionary and if key is found for a paticular task-id, the value is reported as a guage

Add task / key combinations in config.yaml:

```bash
xcom_params:
  -
    task_id: abc
    key: count
  -
    task_id: def
    key: errors

```


a task_id of 'all' will match against all airflow tasks:

```
xcom_params:
 -
    task_id: all
    key: count
```



### Dag Specific Metrics

#### `airflow_dag_status`

Number of DAGs with a specific status.

All the possible states are listed [here](https://github.com/apache/airflow/blob/master/airflow/utils/state.py#L59)

#### `airflow_dag_run_duration`
Duration of successful DagRun in seconds.

### Scheduler Metrics

#### `airflow_dag_scheduler_delay`

Scheduling delay for a DAG Run in seconds. This metric assumes there is a `canary_dag`.

The scheduling delay is measured as the delay between when a DAG is marked as `SCHEDULED` and when it actually starts `RUNNING`.

#### `airflow_task_scheduler_delay`

Scheduling delay for a Task in seconds. This metric assumes there is a `canary_dag`.

#### `airflow_num_queued_tasks`

Number of tasks in the `QUEUED` state at any given instance.
