from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 22),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
dag = DAG(
    'analytical_etl',
    default_args=default_args,
    description='Equity Market ETL',
    schedule_interval=None,
)

task_analytical_etl = BashOperator(
        task_id='spark_transform',
        bash_command="python ./dags/transform.py",
        dag=dag,
    )

task_analytical_etl