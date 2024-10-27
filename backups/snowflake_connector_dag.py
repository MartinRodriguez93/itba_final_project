import os
from pendulum import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from dags.snowflake_etl.functions.snowflake_dag_functions import (
    transform_source_system
)

# VARIABLES DEFINITION ----------------------------------------------------------------------------------------

#Env variables
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow/")
SF_USER = os.environ.get("SF_USER")
SF_ACCOUNT = os.environ.get("SF_ACCOUNT")
SF_PWD = os.environ.get("SF_PWD")
_SNOWFLAKE_CONN_ID = "snowflake_conn"

#Common variables
RUN_DS = '{{ ds }}'
template_searchpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../snowflake_etl/sql")
_SNOWFLAKE_DB = "RAW"
_SNOWFLAKE_SCHEMA = "ORDERS"

#Unique variables
DAG_DICT = {"olist_orders_dataset": ['olist_orders_dataset.csv','olist_orders_{{ ds }}.csv','source_system_order','order_purchase_timestamp'],
            "olist_order_reviews_dataset": ['olist_order_reviews_dataset.csv','olist_order_reviews_{{ ds }}.csv','source_system_order_reviews','review_creation_date'],
            "olist_order_items_dataset": ['olist_order_items_dataset.csv','olist_order_items_{{ ds }}.csv','source_system_order_items','shipping_limit_date'],
            "olist_order_payments_dataset": ['olist_order_payments_dataset.csv','olist_order_payments{{ ds }}.csv','source_system_order_payments','PAYMENT_DATE']
            }

# DAG DEFINITION ----------------------------------------------------------------------------------------

def create_dag(dag_id, default_args, dataset_source, dataset_file, snowflake_table, event_dt_col):
    generated_dag = DAG(dag_id, default_args=default_args, template_searchpath=[template_searchpath])

    with generated_dag:

        # GENERATION ---------------------------------------------------------------------

        # Change KAGGLE_CONFIG_DIR to fetch API keys
        environment_variables = {
            'KAGGLE_CONFIG_DIR': f"{path_to_local_home}/dags",
        }

        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command = f"bash {path_to_local_home}/dags/scripts/download_dataset.sh {path_to_local_home} {dataset_source}",
            env=environment_variables,
        )

      # INGESTION ---------------------------------------------------------------------

        delete_query_task = SQLExecuteQueryOperator(
            task_id="delete_query_task",
            conn_id=_SNOWFLAKE_CONN_ID,
            database=_SNOWFLAKE_DB,
            sql='delete_query.sql',
            params={
                "db_name": _SNOWFLAKE_DB,
                "schema_name": _SNOWFLAKE_SCHEMA,
                "table_name": snowflake_table,
                "ingestion_date": RUN_DS
            }
        )

        transform_source_system_task = PythonOperator(
            task_id="transform_source_system_task",
            python_callable=transform_source_system,
            op_kwargs={
                "src_file": f"{path_to_local_home}/{dataset_source}",
                "file_name": dataset_file,
                "dataset_source": dataset_source,
                "ingestion_date": RUN_DS,
                'SF_USER': SF_USER,
                "SF_ACCOUNT": SF_ACCOUNT,
                "SF_PWD": SF_PWD,
                "database": _SNOWFLAKE_DB,
                "table_name": snowflake_table,
                "event_dt_col": event_dt_col
            },
        )

        download_dataset_task >> delete_query_task >> transform_source_system_task

    return generated_dag

for dataset, param in DAG_DICT.items(): 
    dag_id = "snowflake_{}".format(str(dataset))

    default_args = {
        "owner": "snowflake",
        "start_date": datetime(2017, 10, 2),
        "end_date": datetime(2017, 10, 6),
        "schedule": "@daily",
        "depends_on_past": True, #simulate daily load
        "catchup": True, #simulate daily load
        "max_active_runs": 1,
        "retries": 1,
        "template_searchpath": template_searchpath
    }

    dataset_source = param[0]
    dataset_file = param[1]
    snowflake_table = param[2]
    event_dt_col = param[3]

    globals()[dag_id] = create_dag(dag_id, default_args, dataset_source, dataset_file, snowflake_table, event_dt_col)