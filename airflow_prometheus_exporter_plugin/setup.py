from setuptools import setup

PACKAGE_NAME = "wix-airflow-prometheus-exporter"
PACKAGE_VERSION = "1.0.0"
install_requirements = [
                           'apache-airflow>=1.10.4',
                           'prometheus_client>=0.4.2',
                       ],

extras_require = {
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
    description='Prometheus Exporter for Airflow Metrics. deprecation date: September 2021',
    url='https://github.com/robinhood/airflow_prometheus_exporter',
    install_requires=install_requirements,
    extras_require=extras_require,
    keywords='airflow_prometheus_exporter_plugin',
    py_modules=['prometheus_exporter', 'xcom_config'],
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
            'AirflowPrometheus = airflow_prometheus_exporter_plugin.prometheus_exporter:AirflowPrometheusPlugin'
        ]
    },
)
