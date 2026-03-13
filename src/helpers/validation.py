import os
import pandas as pd
import logging
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from configs.log_configs import setup_logs

setup_logs()

def check_schema_design(df):
    logging.info("Checking inside the check_schema_design ")
    expected_column_names=('Transaction ID','Customer ID', 'Category','Item','Price Per Unit','Quantity','Total Spent','Payment Method','Location','Transaction Date','Discount Applied')
    
    actual_column_length=len(expected_column_names)
    logging.info(f"Expected Column length, {actual_column_length}")

    try:
        if len(df.axes[1])!=actual_column_length:

            logging.error("Column length is not sufficient as the original length")
            raise Exception("Column length is not sufficient as the original length")
        
        df.columns=df.columns.str.strip()

        for col in df.columns:
            if col  not in expected_column_names:

                logging.error("Column name dont match")
                raise Exception("Column name dont match ")
        return {"success":True,"message":"Columns match the schema Design"}
    except Exception as e:
        logging.error(f"Exception part occured for check_schema_desing, {str(e)}")   
        return {"success":False,"message":f"Exception like , {str(e)} "} 

def validate_primary_key(df,column_name):
    logging.info("Inside validate primary key function")

    PRIMARY_KEY="Transaction ID"
    try:

        if column_name!=PRIMARY_KEY:
            logging.error("Wrong Primary Key!!")
            raise Exception("Primary key given is different")
    
        else:
            if df[column_name].isna().any()==True:
                logging.error("The primary key contains the null value")
                raise Exception("The primary key contains null value")
    except Exception as e:
        logging.error(f"Error Exception occured {str(e)}")


