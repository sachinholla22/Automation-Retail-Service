import pandas as pd
import os
from dotenv import load_dotenv
import numpy as np
import json
import shutil 
import sys
from helpers.validation import strip_column_names,check_schema_design,validate_primary_key,check_and_format_data_types_columns

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from configs.log_configs import setup_logs

setup_logs()
from helpers.validation import check_schema_design

load_dotenv()
csv_file="../data/raw/local_retail_copy.csv"

def convert_to_df(csv_path):
    df=pd.read_csv(csv_path)
    return df

df=convert_to_df(csv_file)

df.columns=strip_column_names(df.columns)
res=check_schema_design(df)
primary_res=validate_primary_key(df,"Transaction ID")

df=check_and_format_data_types_columns(df)


print(df.head(5))