import pandas as pd
from google.cloud import storage

def transform_source_system(src_file, ingestion_date, dataset_source, file_name):
    table = pd.read_csv(src_file)

    # Filter datasets by run ds (INGESTION_DATE)
    if dataset_source == 'olist_orders_dataset.csv':
        table['INGESTION_DATE'] = pd.to_datetime(table['order_purchase_timestamp']).dt.date
        table['INGESTION_DATE'] = pd.to_datetime(table['INGESTION_DATE'])
        table = table[(table['INGESTION_DATE'] == ingestion_date)]
    elif dataset_source == 'olist_order_reviews_dataset.csv':
        table['INGESTION_DATE'] = pd.to_datetime(table['review_creation_date']).dt.date
        table['INGESTION_DATE'] = pd.to_datetime(table['INGESTION_DATE'])
        table = table[(table['INGESTION_DATE'] == ingestion_date)]

    # Add UPDATE_TS col
    table['UPDATE_TS'] = pd.Timestamp.utcnow()

    # Convert to CSV to upload to GCS
    table.to_csv(file_name, index=False)
    
    print(f"Succesfuly transformed {file_name}.")
    print(f"{file_name} has a total of {table.shape[0]} rows.")