import pandas as pd
from botocore.exceptions import ClientError
import boto3
from configs.configs import s3_session
from configs.log_configs import setup_logs
import logging

setup_logs()

def upload_to_quarantine(bucket_name,object_name,file_name):
    logging.info("Inside Upload to quarantine function")
    s3=s3_session.client('s3')

    if object_name=='':
        object_name=file_name

    try:
        s3.upload_file(Bucket=bucket_name,Key=object_name,Filename=file_name)
        logging.info("Uploading the quarantine file")
        return  {"sucess":True,"message":"Uploaded the quarantine file successfully"}   
    except Exception as e:
        logging.error(f"Error while uploading the file {str(e)}")  
        return {'success':False,"message":f"Error occured while uploading {str(e)}"}





