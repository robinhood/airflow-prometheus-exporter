FROM python:3.6-slim

ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

RUN set -ex \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        build-essential \
        apt-utils \
    && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow \
    && pip install -U pip

COPY docker/airflow.sh ${AIRFLOW_USER_HOME}/airflow.sh
RUN chmod +x ${AIRFLOW_USER_HOME}/airflow.sh

COPY . /airflow-prometheus-exporter
WORKDIR /airflow-prometheus-exporter
RUN pip install -e .

RUN chown -R airflow: ${AIRFLOW_USER_HOME}

EXPOSE 8080

USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
RUN airflow initdb

CMD ["./airflow.sh"]
