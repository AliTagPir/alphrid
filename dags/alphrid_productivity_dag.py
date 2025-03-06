from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import python_operator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    "owner": "Ali",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    "alphrid_dag",
    default_args=default_args,
    description="A DAG to extract data from Google Sheets and write to a master table",
    schedule_interval=None,  # Trigger manually or set a cron schedule
    start_date=datetime(2023, 1, 1),
    catchup=False,  # Don't backfill previous runs
    tags=["google_sheets", "alphrid"],
) as dag:

    # Dummy task to mark the start of the DAG
    start = DummyOperator(task_id="start")

    # Google Sheets keys to extract data from
    sheet_keys = [
        "sheet_key_1",  # Replace with the actual Google Sheet key
        "sheet_key_2",  # Add more sheet keys as needed
    ]

    # Master table details
    master_sheet_key = "master_sheet_key"  # Replace with the master Google Sheet key
    master_sheet_name = "MasterTable"      # Replace with the name of the master sheet

    # TaskGroup to handle extraction and writing
    google_sheet_group = google_sheet_taskgroup(
        taskgroup_id="google_sheets_extraction_and_writing",
        sheet_keys=sheet_keys,
        master_sheet_key=master_sheet_key,
        master_sheet_name=master_sheet_name,
    )

    # Dummy task to mark the end of the DAG
    end = DummyOperator(task_id="end")

    # Define dependencies
    start >> google_sheet_group >> end
