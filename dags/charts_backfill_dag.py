from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airfow.operators.python import PythonOperator, task
from airflow.models import Variable

from plugins.custom_operators.backfill_charts_operators import (
    CheckMissingChartsOperator, 
    GenerateMissingChartsOperator
)

default_args = {
    'owner': 'ali',
    'retries': 1,
}

with DAG(
    dag_id='charts_backfill_dag',
    description='Backfill missing daily, weekly, monthly charts in chart_cache',
    default_args=default_args,
    schedule_interval=None,       # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    concurrency=10,
    tags=['charts', 'backfill']
) as dag:
    
    start = EmptyOperator(task_id="initialise_dag")

    check_missing = CheckMissingChartsOperator(
        task_id="check_missing_charts"
    )

    @task
    def generate_missing_chart(chart_key: str):
        return GenerateMissingChartsOperator(chart_key=chart_key).execute({})

    daily_tasks = generate_missing_chart.expand(chart_key=check_missing.output['daily'])
    weekly_tasks = generate_missing_chart.expand(chart_key=check_missing.output['weekly'])
    monthly_tasks = generate_missing_chart.expand(chart_key=check_missing.output['monthly'])


    end = EmptyOperator(task_id="terminate_dag")

    start >> check_missing
    check_missing >> [daily_tasks, weekly_tasks, monthly_tasks]
    [daily_tasks, weekly_tasks, monthly_tasks] >> end