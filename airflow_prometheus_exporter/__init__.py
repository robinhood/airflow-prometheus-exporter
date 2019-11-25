# -*- coding: utf-8 -*-

"""Top-level package for Airflow Prometheus Exporter."""

__author__ = """Robinhood Markets, Inc."""
__email__ = 'open-source@robinhood.com'
__version__ = '1.0.4'


import yaml
import os

dir = os.path.dirname(__file__)
filename = os.path.join(dir, '../config.yaml')


xcom_config = {}
with open(filename) as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    xcom_config = yaml.load(file, Loader=yaml.FullLoader)
