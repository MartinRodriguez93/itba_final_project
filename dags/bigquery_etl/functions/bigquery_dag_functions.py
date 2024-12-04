import pandas as pd
from google.cloud import storage

def transform_source_system(src_file, 
                            file_name,
                            ingestion_date,
                            event_dt_col
                            ):

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

    # Convert to CSV to upload to GCS
    table.to_csv(file_name, index=False)
    
    print(f"Succesfuly transformed {file_name}.")
    print(f"{file_name} has a total of {table.shape[0]} rows.")