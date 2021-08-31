#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

PACKAGE_NAME = "wix_airflow_prometheus_exporter"
PACKAGE_VERSION = "1.0.0"
install_requirements = [
    'apache-airflow>=1.10.4',
    'prometheus_client>=0.4.2',
],

extras_require={
    'dev': [
        'bumpversion',
        'tox',
        'twine',
    ]
}

setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    author='Robinhood Markets, Inc. Forked by Roy Noyman',
    author_email='Royno@wix.com',
    description='Prometheus Exporter for Airflow Metrics',
    install_requires=install_requirements,
    extras_require=extras_require,
    keywords='wix_airflow_prometheus_exporter',
    packages=['wix_airflow_prometheus_exporter'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    entry_points={
        'airflow.plugins': [
            'AirflowPrometheus = wix_airflow_prometheus_exporter.prometheus_exporter:AirflowPrometheusPlugin'
        ]
    },
)
