import yaml
import os

dir = os.path.dirname(__file__)
filename = os.path.join(dir, "../config.yaml")


xcom_config = {}
with open(filename) as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    xcom_config = yaml.load(file, Loader=yaml.FullLoader)
