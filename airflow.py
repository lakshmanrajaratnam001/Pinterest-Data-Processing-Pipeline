from os.path import expanduser
import os
import sys
from pathlib import Path
from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
import spark


default_args = {
    'owner': 'Lakshman',
    'depends_on_past': False,
    'email': ['lakshmanrajaratnam@outlook.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2022, 1, 9),
    'retry_delay': timedelta(minutes=1),
    'end_date': datetime(2022, 2, 9),
}

spark_session = spark.spark_session()
with DAG(dag_id='dag_spark',
         default_args=default_args,
         schedule_interval='11 15 * * *',
         catchup=False,
         tags=['test']
         ) as dag:
    # Define the tasks. Here we are going to define only one bash operator
    test_task = BashOperator(
        task_id='run_spark',
        session = spark_session)