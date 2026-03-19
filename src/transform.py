import pandas as pd
import os
from dotenv import load_dotenv
import numpy as np
import sys
from src.helpers.validation import strip_column_names,check_schema_design,validate_primary_key,check_and_format_data_types_columns
from src.extract import create_qurantine_csv_file
from src.load import upload_to_quarantine
from configs.log_configs import setup_logs

def transform():
    setup_logs()


    load_dotenv()
    csv_file="/opt/airflow/data/raw/local_retail_copy.csv"

    def convert_to_df(csv_path):
        df=pd.read_csv(csv_path)
        return df

    is_correct_schema=True
    df=convert_to_df(csv_file)

    df.columns=strip_column_names(df.columns)

    schema_result=check_schema_design(df)
    if schema_result['success']==False:
        is_correct_schema=False
        quarantine_file_name='invalid_schema.csv'
        quarantine_path="/opt/airflow/data/raw/quarantine"
        create_qurantine_csv_file(df,os.path.join(quarantine_path,quarantine_file_name))
        upload_to_quarantine(os.getenv('S3_BUCKET'),os.getenv('S3_QUARANTINE_SCHEMA_OBJECT'),os.path.join(quarantine_path,quarantine_file_name))




    if is_correct_schema:
        primary_res=validate_primary_key(df)
        df=check_and_format_data_types_columns(df)



