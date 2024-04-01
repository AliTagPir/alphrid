from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Ali',
    'retry': 3,
    'retry_delay': timedelta(minutes=5)
}

def get_pandas():
    import pandas
    print(f"pandas version: {pandas.__version__}")


with DAG(
    default_args=default_args,
    dag_id='testing_requirements_dag',
    start_date=datetime(2024, 3, 31),
    schedule_interval='@daily'
) as dag:

    get_pandas = PythonOperator(
        task_id='get_pandas',
        python_callable=get_pandas
    )

    get_pandas