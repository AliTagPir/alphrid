from airflow import DAG
from datetime import datetime
from custom_operators.tracker_operators import ExtractGSDataOperator, TransformAndLoadOperator, PurgeOldDailyChartsOperator
from custom_operators.generate_charts_operator import GenerateChartsOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator 

default_args = {
    "owner": "Ali",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="alphrid_productivity_tracker_dag",
    default_args=default_args,
    start_date=datetime(2024, 3, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=['alphrid', 'productivity']
) as dag:
    
    initialise_dag = EmptyOperator(
        task_id="initialise_dag",
        wait_for_downstream=False
    )

    extract_task = ExtractGSDataOperator(
        task_id="extract_google_sheets_data",
        worksheet_name="Daily_Input"
    )

    transform_and_load_task = TransformAndLoadOperator(
        task_id="transform_and_load_tracker_data",
        pg_conn_id="external_alphrid_db"
    )

    generate_charts_task = GenerateChartsOperator(
        task_id="generate_charts",
        pg_conn_id="external_alphrid_db"
    )

    purge_old_charts_task = PurgeOldDailyChartsOperator(
        task_id="purge_old_daily_charts",
        pg_conn_id="external_alphrid_db",
        retention_days=14
    )

    terminate_dag = EmptyOperator(
        task_id="terminate_dag"
    )

initialise_dag >> extract_task >> transform_and_load_task >> generate_charts_task >> purge_old_charts_task >> terminate_dag

