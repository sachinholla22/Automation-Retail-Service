import os
import pandas as pd
import logging
import sys
from typing import List,Dict


sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from configs.configs import PRIMARY_KEY,Column_dtype_dictionary,EXPECTED_COLUMN_NAMES
from configs.log_configs import setup_logs

setup_logs()

#Function to check the schema Design 
def check_schema_design(df):
    logging.info("Checking inside the check_schema_design ")
    expected_column_names=EXPECTED_COLUMN_NAMES
    
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

#Function to validate the primary key
def validate_primary_key(df,column_name:str)->Dict[str,any]:
    logging.info("Inside validate primary key function")

    try:

        if column_name!=PRIMARY_KEY:
            logging.error("Wrong Primary Key!!")
            raise Exception("Primary key given is different")
    
        else:
            if df[column_name].isna().any()==True:
                logging.error("The primary key contains the null value")
                raise Exception("The primary key contains null value")
        return {"success":True,"message":"Primary key is valid"}    
    except Exception as e:
        logging.error(f"Error Exception occured {str(e)}")
        return {"success":False,"message":f"Error occured ,{str(e)}"}

def check_and_format_data_types_columns(df):
    logging.info("Inside the function check_data_types_columns")
    incorrect_dtypes_columns=[]

    for key,value in df.dtypes.items():
        is_correct_dtype=True

        if value!=Column_dtype_dictionary.get(key):
            logging.warning(f"Found the incorrect dtype column {key}")
            is_correct_dtype=False
        if not is_correct_dtype:
            incorrect_dtypes_columns.append(key)

            
    if len(incorrect_dtypes_columns)>0:
        for col in  incorrect_dtypes_columns:
            df[col]=df[col].astype(Column_dtype_dictionary.get(col))
    return df        
            
           

       

    

def strip_column_names(columns:List[str])->List[str]:
    logging.info("Stripping the column names")
    return columns.str.strip()




    

