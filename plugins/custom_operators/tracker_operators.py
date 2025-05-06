import os
import pandas as pd
import gspread
import json
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv
from datetime import datetime

load_dotenv("/opt/airflow/.env")


class ExtractGSDataOperator(BaseOperator):
    
    def __init__(self, worksheet_name, *args, **kwargs):
        super(ExtractGSDataOperator,self).__init__(*args,**kwargs)
        self.spreadsheet_name = os.getenv('SPREADSHEET_NAME')
        self.worksheet_name = worksheet_name
        self.gs_creds_var = os.getenv('GS_CREDS_VAR')


    def get_gs_client(self):
        if not self.gs_creds_var:
            raise ValueError("GS_CREDS_VAR is missing or empty.")
        gs_creds_dict = json.loads(self.gs_creds_var)
        client = gspread.service_account_from_dict(gs_creds_dict)
        return client

    def read_gs_data(self,client):
        sheet = client.open(self.spreadsheet_name).worksheet(self.worksheet_name)
        
        # Read the data from google sheet
        raw_daily_data = sheet.get("A1:B5")

        # Clear google sheet ~~~~~~~~~~~~~~~~~~~ Commented out for testing 
        # self.log.info("Clearing Daily Input")
        # sheet.clear()

        # # Template for 
        # reset_data = {
        #     "Activity": ["Working", "Programming", "Exercise", "Leisure"],
        #     "Hours": ["", "", "", ""]
        # }

        # # Convert dict to row-wise format (list of lists)
        # rows = [list(reset_data.keys())]  # Header row
        # for row in zip(*reset_data.values()):
        #     rows.append(list(row))

        # # Push it to the sheet starting at A1
        # sheet.update("A1", rows)
        # self.log.info("Reset headers and index labels in sheet.")
        return raw_daily_data

    
    def execute(self,context):
        self.log.info('Extracting productivity data from Google Sheet')
        self.log.info(self.spreadsheet_name)
        self.log.info(f"GS_CREDS_VAR: {self.gs_creds_var is not None}")
        
        client = self.get_gs_client()
        raw_daily_data = self.read_gs_data(client)
        
        self.log.info(f"Data extracted: {raw_daily_data}")
        raw_gs_df = pd.DataFrame(raw_daily_data[1:], columns=raw_daily_data[0])
        # raw_gs_df = pd.DataFrame(raw_daily_data)
        # preview_gs_data = raw_gs_df.to_dict(orient="records")
        # self.log.info(f"Data preview before XCOM push: {preview_gs_data}")
        return raw_gs_df.to_dict(orient="records")



class TransformAndLoadOperator(BaseOperator):
    
    def __init__(self, pg_conn_id, *args, **kwargs):
        super().__init__(*args,**kwargs)
        self.pg_conn_id = pg_conn_id

    def transform_data(self, raw_data):
        
        """
        Reformat raw data (list of dicts) to a single row matching master_tracker table 
        """

        df = pd.DataFrame(raw_data)
        self.log.info(f"Raw DataFrame: {df}")
        df = df.set_index("Activity")
        df.index = df.index.str.lower()

        row = {
            "date": datetime.today().date(),
            "working": float(df.at["working", "Hours"]) if "working" in df.index else 0.0,
            "programming": float(df.at["programming", "Hours"]) if "programming" in df.index else 0.0,
            "exercise": float(df.at["exercise", "Hours"]) if "exercise" in df.index else 0.0,
            "leisure": float(df.at["leisure", "Hours"]) if "leisure" in df.index else 0.0,
        }

        return row
    
    def load_data(self, row_dict):
        """
        Inserts or Upserts the row into the master_tracker_data table
        """

        hook = PostgresHook(postgres_conn_id=self.pg_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        insert_sql = """
            INSERT INTO master_tracker_data (date, working, programming, exercise, leisure)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (date)
            DO UPDATE SET
                working = EXCLUDED.working,
                programming = EXCLUDED.programming,
                exercise = EXCLUDED.exercise,
                leisure = EXCLUDED.leisure;
        """

        cursor.execute(insert_sql, (
            row_dict["date"],
            row_dict["working"],
            row_dict["programming"],
            row_dict["exercise"],
            row_dict["leisure"],
        ))
        self.log.info(f"{cursor.rowcount} row(s) affected.")
        
        conn.commit()
        cursor.close()
        conn.close()

    def execute(self, context):
        self.log.info("Starting data transformation and loading")

        #Pull raw data from previous node
        raw_data = context["ti"].xcom_pull(task_ids="extract_google_sheets_data")
        self.log.info(f"Raw data pulled:{raw_data}")
        
        #Transorm raw data to the right format
        row_dict = self.transform_data(raw_data)
        self.log.info(f"Transformed row: {row_dict}")

        #Connect and load processed data to external_alphrid_db (specifically master_tracker_data table)
        self.load_data(row_dict)
        self.log.info("Data was successfully inserted into master_tracker_data table")
