'''
To do list

- extract_gs_data
    ~connect to gs
    ~read data
    ~process data

- write_to_database (temporary)
    ~connect to db (gs for temp)
    ~write to gs as temp db

- generate_diagrams
    ~read db
    ~parse data 
    ~generate updated diagram
'''
import os
import pandas as pd
import gspread
import json
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

class Extract_gs_data(BaseOperator):
    
    @apply_defaults
    def __init__(self, 
                *args,
                **kwargs):
        super(Extract_gs_data, self).__init__(*args, **kwargs)
        self.sheet_key = os.getenv('GS_KEY')
        self.sheet_name = os.getenv('SHEET_NAME')
        self.gs_credentials = os.getenv('GS_CREDENTIALS')
    
    def get_gs_client(self):
        gs_cred_dict = json.loads(self.gs_credentials)
        gs_cred = Credentials.from_service_account_info(gs_cred_dict)
        client = gspread.authorize(gs_cred)
        return client
    
    def read_gs_data(self, client):
        sheet = client.open_by_key(self.sheet_key).worksheet(self.sheet_name)
        daily_data = sheet.get_all_values()
        return daily_data

    def clear_sheet_data(self, sheet):
        # Get the number of rows and columns
        num_rows = len(sheet.get_all_values())
        num_columns = len(sheet.row_values(1))
        
        ranges_to_clear = []

        # Clear from row 2 to the last row, all columns
        if num_rows > 1:
            ranges_to_clear.append(f'B2:{gspread.utils.rowcol_to_a1(num_rows, num_columns)}')
        
        for cell_range in ranges_to_clear:
            sheet.batch_clear([cell_range])
        
    
    def execute(self, context):
        self.log.info('Extracting productivity data from Google Sheet')
        client = self.get_gs_client()
        data = self.read_gs_data(client)
        self.log.info{f'Retrieved data: {daily_data}'}
        daily_prod_df = pd.DataFrame(daily_data)

        return daily_prod_df

class WriteToMasterTable(BaseOperator):
    
    @apply_defaults
    def __init__(self, sheet_key, sheet_name, data, *args, **kwargs):
        """
        Args:
            sheet_key (str): Google Sheet key where data will be written.
            sheet_name (str): Name of the sheet/page within the Google Sheet.
            data (list or DataFrame): Data to write to the sheet.
        """
        super(WriteToMasterTable, self).__init__(*args, **kwargs)
        self.sheet_key = sheet_key
        self.sheet_name = sheet_name
        self.data = data
        self.gs_credentials = os.getenv('GS_CREDENTIALS')
    
    def get_gs_client(self):
        gs_cred_dict = json.loads(self.gs_credentials)
        gs_cred = Credentials.from_service_account_info(gs_cred_dict)
        client = gspread.authorize(gs_cred)
        return client
    
    def write_gs_data(self, client):
        sheet = client.open_by_key(self.sheet_key)
        
        # Open the specified worksheet or create it if it doesn't exist
        try:
            worksheet = sheet.worksheet(self.sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            self.log.info(f"Worksheet {self.sheet_name} not found. Creating it.")
            worksheet = sheet.add_worksheet(title=self.sheet_name, rows="100", cols="20")
        
        # Convert DataFrame to list if necessary
        if isinstance(self.data, pd.DataFrame):
            data_to_write = [self.data.columns.tolist()] + self.data.values.tolist()
        else:
            data_to_write = self.data

        # Write the data to the worksheet
        worksheet.clear()  # Clear existing data
        worksheet.update("A1", data_to_write)

    def execute(self, context):
        self.log.info('Writing data to the master table in Google Sheet.')
        client = self.get_gs_client()
        self.write_gs_data(client)
        self.log.info('Data successfully written to Google Sheet.')

    
    '''
    TO DO 
    - clear the data after reading
    - transform df (time stamp and reformat) 
    - xcom data to next step
    ~ potential to reformat SHEET_NAME to be set at the DAG





