
from airflow.decorators import dag, task
from datetime import datetime 

from src.extract import extract_from_s3,create_qurantine_csv_file

from src.transform import transform
from src.load import upload_to_quarantine
import os
from dotenv import load_dotenv
from configs.log_configs import setup_logs
import logging


setup_logs()
load_dotenv()


local_path='/opt/airflow/data/raw'
local_file='local_retail_copy.csv'

local_full_path=os.path.join(local_path,local_file)

s3_bucket_name=os.getenv('S3_BUCKET')
s3_object_name=os.getenv('S3_OBJECT')

@dag(
   dag_id="etl_de",
   start_date=datetime(2026,3,9),
   catchup=False,
   schedule="@daily",
   tags=['learn_airflow'],

)

def etl_process():

    @task(task_id="extract_data")
    def extract():
        
        extract_from_s3(s3_bucket_name,s3_object_name,local_full_path)

    @task(task_id="transform")
    def transforming():
        transform()    

    extract() >> transforming()

etl_process()





    
    
    

