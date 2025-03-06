from airflow.utils.task_group import TaskGroup
from plugins.custom_operators.productivity_operators import Extract_gs_data, WriteToMasterTable

def google_sheet_taskgroup(taskgroup_id, sheet_keys, master_sheet_key, master_sheet_name):
    """
    Creates a TaskGroup to extract data from multiple Google Sheets and write to a master sheet.

    Args:
        taskgroup_id (str): Unique ID for the TaskGroup.
        sheet_keys (list): List of Google Sheet keys (IDs) to extract data from.
        master_sheet_key (str): Google Sheet key where the master table resides.
        master_sheet_name (str): Name of the sheet/page within the master table sheet.

    Returns:
        TaskGroup: The TaskGroup object containing tasks for each sheet.
    """
    with TaskGroup(taskgroup_id=taskgroup_id, tooltip="Google Sheets Extraction and Writing") as group:
        extracted_data_tasks = []

        for i, sheet_key in enumerate(sheet_keys):
            # Task to extract data from each Google Sheet
            extract_task = Extract_gs_data(
                task_id=f"extract_sheet_{i + 1}",
                sheet_key=sheet_key,
            )
            extracted_data_tasks.append(extract_task)

        # Task to write all extracted data to the master table
        write_to_master = WriteToMasterTable(
            task_id="write_to_master_table",
            sheet_key=master_sheet_key,
            sheet_name=master_sheet_name,
            data="{{ task_instance.xcom_pull(task_ids=[task.task_id for task in extracted_data_tasks]) }}",
        )

        # Set dependencies: all extract tasks must finish before writing
        extracted_data_tasks >> write_to_master

    return group

