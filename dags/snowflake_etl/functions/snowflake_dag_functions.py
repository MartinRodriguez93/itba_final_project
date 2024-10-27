from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import json
import snowflake.connector
from datetime import datetime

def transform_source_system(src_file, 
                            file_name, 
                            dataset_source, 
                            ingestion_date, 
                            SF_USER, 
                            SF_ACCOUNT, 
                            SF_PWD, 
                            db_name, 
                            table_name,
                            event_dt_col
                            ):
    
    con = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PWD,
            account=SF_ACCOUNT,
            warehouse='COMPUTE_WH',
            database='RAW',
            schema='ORDERS'
        )
    cursor = con.cursor()

    # Fetch data
    table = pd.read_csv(src_file)

    # Data transformation
    # In this script, we are only tackling transformations necessary to upload to Snowflake.
    # Data quality transformations will be performed in Snowflake.

    # Filter datasets by run ds (INGESTION_DATE)
    table['INGESTION_DATE'] = pd.to_datetime(table[event_dt_col]).dt.date
    table['INGESTION_DATE'] = pd.to_datetime(table['INGESTION_DATE'])
    table = table[(table['INGESTION_DATE'] == ingestion_date)]

    # Add UPDATE_TS col
    table['UPDATE_TS'] = pd.Timestamp.utcnow()

    print(f"Succesfuly transformed {file_name}.")
    print(f"{file_name} has a total of {table.shape[0]} rows.")

    con.cursor().execute("use warehouse COMPUTE_WH")

    # Upload data to snowflake
    success, num_chunks, num_rows, output = write_pandas(conn=con,
                                            df=table, 
                                            table_name=table_name,
                                            database='RAW',
                                            schema='ORDERS',
                                            on_error='CONTINUE',
                                            quote_identifiers=False,
                                            auto_create_table=True
                                            )

    print(f"Succesfuly uploaded {file_name} to snowflake.") 
    print(f"Data inserted successfully: {success}, Number of rows inserted: {num_rows}")