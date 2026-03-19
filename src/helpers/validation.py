import os
import pandas as pd
import logging
import sys
from typing import List,Dict
import yaml
from configs.configs import PRIMARY_KEY
from configs.log_configs import setup_logs

setup_logs()


#function for converting the yaml into schema key values
def convert_yaml_schema(file_name:str):
    if not file_name.endswith(".yaml") or not file_name.endswith(".yaml"):
        logging.error("The uploaded file is not a valid yaml file")
        raise Exception("The uploaded file is not a valid yaml file")
    else:
        with open(file_name,'r')as file:
            logging.info("openeing yaml schema file")
            schema=yaml.safe_load(file)
    return schema   
schema=convert_yaml_schema('/opt/airflow/schema.yaml')     

#Function to check the schema Design 
def check_schema_design(df):
    logging.info("Checking inside the check_schema_design ")
    
    expected_column_names=schema['expected_columns']
    
    actual_column_length=len(expected_column_names)
    logging.info(f"Expected Column length, {actual_column_length}")
    
    try:
        if len(df.axes[1])!=actual_column_length:

            logging.error("Column length is not sufficient as the original length")
            raise Exception("Column length is not sufficient as the original length")
        
        df.columns=df.columns.str.strip()

        for col in df.columns:
            if col  not in expected_column_names:

                logging.error(f"Column name dont match {col}")
                raise Exception(f"Column name dont match {col}")
        logging.info("Columns are having expected")    
        return {"success":True,"message":"Columns match the schema Design"}
    except Exception as e:
        logging.error(f"Exception part occured for check_schema_desing, {str(e)}")   
        return {"success":False,"message":f"Exception like , {str(e)} "} 

#Function to validate the primary key
def validate_primary_key(df)->Dict[str,any]:
    logging.info("Inside validate primary key function")

    try:
        column_name=df['Transaction ID']

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

def check_and_format_data_types_columns(df)->pd.DataFrame:
    logging.info("Inside the function check_data_types_columns")
    incorrect_dtypes_columns=[]
    Column_dtype_dictionary=schema['column_data_types']
    for key,value in df.dtypes.items():
        is_correct_dtype=True

        if value!=Column_dtype_dictionary.get(key):
            logging.warning(f"Found the incorrect dtype column {key}")
            is_correct_dtype=False
        if not is_correct_dtype:
            incorrect_dtypes_columns.append(key)
    if len(incorrect_dtypes_columns)>0:
        convert_to_datatypes(df,incorrect_dtypes_columns)
    return df    
            
     
            
def convert_to_datatypes(df:pd.DataFrame,col_list:List[str]): 
    logging.info("Inside the converting to datatype function")

    dtypes_list=schema['column_data_types']
    
    for items in col_list:
        try:

            if dtypes_list.get(items)=='datetime':
                df[items]=pd.to_datetime(df[items],errors='coerce')
            else:
                df[items]=df[items].astype(dtypes_list.get(items))
        except Exception as e:
            logging.error(f"Couldnt convert the data type {str(dtypes_list.get)}")


def strip_column_names(columns:List[str])->List[str]:
    logging.info("Stripping the column names")
    return columns.str.strip()




    

