#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import find_packages, setup

with open("README.md", encoding="utf-8") as readme_file:
    readme = readme_file.read()

install_requirements = (
    [
        "apache-airflow==1.10.12",
        "croniter==0.3.36",
        "dateparser==1.0.0",
        "prometheus_client==0.8.0",
        "pendulum==1.4.4",
        "importlib_metadata==1.7.0",
        "numpy==1.18.1",
        "marshmallow-sqlalchemy==0.23.1",
        "marshmallow==2.21.0",
    ],
)  # noqa

extras_require = {"dev": ["bumpversion", "tox", "twine"]}  # noqa

setup(
    author="Robinhood Markets, Inc.",
    author_email="open-source@robinhood.com",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    description="Prometheus Exporter for Airflow Metrics",
    install_requires=install_requirements,
    extras_require=extras_require,
    license="BSD 3-Clause",
    long_description=readme,
    long_description_content_type="text/markdown",
    keywords="airflow_prometheus_exporter",
    name="airflow_prometheus_exporter",
    packages=find_packages(include=["airflow_prometheus_exporter"]),
    include_package_data=True,
    url="https://github.com/robinhood/airflow_prometheus_exporter",
    version="1.0.7",
    entry_points={
        "airflow.plugins": [
            "AirflowPrometheus = airflow_prometheus_exporter.prometheus_exporter:AirflowPrometheusPlugin"
        ]
    },
)
