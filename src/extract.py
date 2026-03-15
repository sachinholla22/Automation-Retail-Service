import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from configs.log_configs import setup_logs
import logging
from configs.configs import s3_session

load_dotenv()
setup_logs()




def extract_from_s3(bucket_name,object_name,local_destination):
    logging.info("Inside the download function")
    
    if   os.path.exists(local_destination) and os.path.getsize(local_destination)>0:
        logging.info("File already exists and contains the contents")
        return 
        
    s3=s3_session.client('s3')
    try:
        s3.download_file(bucket_name,object_name,local_destination)
        logging.info(f'File has been successfully downloaded to the local_path {local_destination}')
    except ClientError as e:
        logging.error('S3 download failed',exc_info=True)
        raise 

      

