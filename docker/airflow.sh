#!/bin/bash
AF_HOME="/usr/local/airflow"
sqlite3 "${AF_HOME}/airflow.db" "create table gap_dag_tag(
    dag_id varchar(250) PRIMARY KEY,
	task_id varchar(250),
	cadence varchar(250),
	severerity varchar(250),
	alert_target varchar(250),
	instant_slack_alert varchar(250),
    alert_classification varchar(250),
    sla_interval float,
    sla_time text,
    latest_successful_run text);"
airflow webserver
