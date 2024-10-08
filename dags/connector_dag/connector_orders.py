import os
from pendulum import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyTableOperator
)

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from dags.connector_dag.functions.connector_orders_functions import (
    transform_source_system,
    upload_to_gcs
)

# VARIABLES DEFINITION ----------------------------------------------------------------------------------------

#Env variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow/")
LOCATION = os.environ.get("LOCATION", 'US')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'source_dataset')

#Common variables
RUN_DS = '{{ ds }}'

#Unique variables
DAG_DICT = {"olist_orders_dataset": ['olist_orders_dataset.csv','olist_orders_{{ ds }}.csv','source_system_orders'],
            "olist_order_reviews_dataset": ['olist_order_reviews_dataset.csv','olist_order_reviews_{{ ds }}.csv','source_system_order_reviews']
            }

# DAG DEFINITION ----------------------------------------------------------------------------------------

def create_dag(dag_id, default_args, dataset_source, dataset_file, bigquery_table):

    generated_dag = DAG(dag_id, default_args=default_args)

    with generated_dag:

        # Change KAGGLE_CONFIG_DIR to fetch API keys
        environment_variables = {
            'KAGGLE_CONFIG_DIR': f"{path_to_local_home}/dags",
        }

        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command = f"bash {path_to_local_home}/dags/scripts/download_dataset.sh {path_to_local_home} {dataset_source}",
            env=environment_variables,
        )

        transform_source_system_task = PythonOperator(
            task_id="transform_source_system_task",
            python_callable=transform_source_system,
            op_kwargs={
                "src_file": f"{path_to_local_home}/{dataset_source}",
                "file_name": dataset_file,
                "dataset_source": dataset_source,
                "ingestion_date": RUN_DS
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{dataset_file}",
                "local_file": f"{path_to_local_home}/{dataset_file}",
            },
        )

        DELETE_QUERY = (
            f"DELETE {BIGQUERY_DATASET}.{BIGQUERY_TABLE} \
            WHERE 1=1 \
            AND INGESTION_DATE = '{RUN_DS}';"
        )

        delete_query_task = BigQueryInsertJobOperator(
            task_id="delete_query_task",
            configuration={
                "query": {
                    "query": DELETE_QUERY,
                    "useLegacySql": False,
                    "priority": "BATCH",
                }
            },
            location=LOCATION,
            deferrable=True,
        )

        append_rows_task = GoogleCloudStorageToBigQueryOperator(
            task_id="append_rows_task",
            bucket=BUCKET,
            source_objects=[f"raw/{dataset_file}"],
            destination_project_dataset_table=f"{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
            schema_object=f"source_systems_schemas/{bigquery_table}.json",
            source_format='CSV',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_APPEND',
            field_delimiter=',',
            skip_leading_rows=1,
            allow_quoted_newlines=True, #Parse csv cols with comments
            autodetect=False
        )

        download_dataset_task >> transform_source_system_task >> local_to_gcs_task >> delete_query_task >> append_rows_task

    return generated_dag

for dataset, param in DAG_DICT.items(): 
    dag_id = "connector_{}".format(str(dataset))

    default_args = {
        "owner": "airflow",
        "start_date": datetime(2017, 10, 2),
        "end_date": datetime(2017, 10, 6),
        "schedule": "@daily",
        "depends_on_past": True, #simulate daily load
        "catchup": True, #simulate daily load
        "max_active_runs": 1,
        "retries": 1,
    }

    dataset_source = param[0]
    dataset_file = param[1]
    BIGQUERY_TABLE = param[2]

    globals()[dag_id] = create_dag(dag_id, default_args, dataset_source, dataset_file, BIGQUERY_TABLE)