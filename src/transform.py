import pandas as pd
import os
from dotenv import load_dotenv
import numpy as np
load_dotenv()
csv_file="../data/raw/local_retail_copy.csv"

def read_csv(file_path):
    df=pd.read_csv(file_path)
    return df

df=read_csv(csv_file)
# print(df.set_index('Customer ID'))
# print(df.head

print(df.loc[1])

df1=pd.DataFrame(
    {
        "nums":[3,5,6,9,14],
        "names":['sachin','shewag','dhoni','kohli','modi']
    }
)
df1.drop(["nums"],inplace=True,axis=1)

df1.rename(columns={"names":"Names"},inplace=True)
# print(new_data)

# print(df2)