#!/bin/bash
AF_HOME="/usr/local/airflow"
sqlite3 "${AF_HOME}/airflow.db" "create table delay_alert_metadata(
    dag_id varchar(250) PRIMARY KEY,
	task_id varchar(250),
	cadence varchar(250),
	severity varchar(250),
	alert_target varchar(250),
    alert_external_classification varchar(250),
    alert_report_classification varchar(250),
    sla_interval float,
    sla_time text,
    latest_successful_run text);"
airflow webserver
