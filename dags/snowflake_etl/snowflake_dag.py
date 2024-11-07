import os
from pendulum import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from dags.snowflake_etl.functions.snowflake_dag_functions import (
    transform_source_system,
    check_row_count
)

# VARIABLES DEFINITION ----------------------------------------------------------------------------------------

# Env variables
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow/")
SF_USER = os.environ.get("SF_USER")
SF_ACCOUNT = os.environ.get("SF_ACCOUNT")
SF_PWD = os.environ.get("SF_PWD")
SF_CONN_ID = "snowflake_conn"

# Common variables
RUN_DS = '{{ ds }}'
TEMPLATE_SEARCHPATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../snowflake_etl/sql")
SF_RAW_DB = "RAW"
SF_STAGING_DB = "STAGING"
SF_SCHEMA = "ORDERS"

# Unique variables
DAG_DICT = {
    "olist_orders_dataset": ['olist_orders_dataset.csv', 'olist_orders_{{ ds }}.csv', 'source_system_order', 'order_purchase_timestamp','stg_olist__orders.sql'],
    "olist_order_reviews_dataset": ['olist_order_reviews_dataset.csv', 'olist_order_reviews_{{ ds }}.csv', 'source_system_order_reviews', 'review_creation_date','stg_olist__order_reviews.sql'],
    "olist_order_items_dataset": ['olist_order_items_dataset.csv', 'olist_order_items_{{ ds }}.csv', 'source_system_order_items', 'shipping_limit_date','stg_olist__order_items.sql'],
    "olist_order_payments_dataset": ['olist_order_payments_dataset.csv', 'olist_order_payments{{ ds }}.csv', 'source_system_order_payments', 'PAYMENT_DATE','stg_olist__order_payments.sql']
}

# DAG DEFINITION ----------------------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "snowflake",
    "start_date": datetime(2017, 10, 2),
    "end_date": datetime(2017, 10, 6),
    "schedule": "@daily",
    "depends_on_past": True,  # simulate daily load
    "catchup": True,  # simulate daily load
    "max_active_runs": 1,
    "retries": 1,
    "template_searchpath": TEMPLATE_SEARCHPATH
}

with DAG('snowflake_dag', default_args=DEFAULT_ARGS, template_searchpath=[TEMPLATE_SEARCHPATH]) as dag:

    staging_tasks = []
    
    for dataset, params in DAG_DICT.items():
        DATASET_SOURCE, DATASET_FILE, TABLE_NAME, EVENT_DT_COL, STAGING_QUERY = params

        # GENERATION ---------------------------------------------------------------------

        download_task = BashOperator(
            task_id=f"download_{dataset}",
            bash_command=f"bash {PATH_TO_LOCAL_HOME}/dags/scripts/download_dataset.sh {PATH_TO_LOCAL_HOME} {DATASET_SOURCE}"
        )

        # INGESTION ---------------------------------------------------------------------

        delete_query_task = SQLExecuteQueryOperator(
            task_id=f"delete_query_{dataset}",
            conn_id=SF_CONN_ID,
            database=SF_RAW_DB,
            sql='delete_query.sql',
            params={
                "raw_db_name": SF_RAW_DB,
                "schema_name": SF_SCHEMA,
                "table_name": TABLE_NAME,
                "event_dt_col": EVENT_DT_COL
            }
        )

        transform_task = PythonOperator(
            task_id=f"transform_{dataset}",
            python_callable=transform_source_system,
            op_kwargs={
                "src_file": f"{PATH_TO_LOCAL_HOME}/{DATASET_SOURCE}",
                "file_name": DATASET_FILE,
                "dataset_source": DATASET_SOURCE,
                "ingestion_date": RUN_DS,
                'SF_USER': SF_USER,
                "SF_ACCOUNT": SF_ACCOUNT,
                "SF_PWD": SF_PWD,
                "db_name": SF_RAW_DB,
                "table_name": TABLE_NAME,
                "event_dt_col": EVENT_DT_COL
            },
        )

        unique_query_task = PythonOperator(
            task_id=f"unique_query_{dataset}",
            python_callable=check_row_count,
            op_kwargs={
                'SF_USER': SF_USER,
                "SF_ACCOUNT": SF_ACCOUNT,
                "SF_PWD": SF_PWD,
                "db_name": SF_RAW_DB,
                "table_name": TABLE_NAME,
                "template_searchpath": f"{TEMPLATE_SEARCHPATH}/unique_query_{dataset}.sql",
                "params": {
                    "raw_db_name": SF_RAW_DB,
                    "schema_name": SF_SCHEMA,
                    "table_name": TABLE_NAME,
                    "event_dt_col": EVENT_DT_COL
                },
            },
            provide_context=True
        )

        staging_query_task = SQLExecuteQueryOperator(
            task_id=f"staging_query_{dataset}",
            conn_id=SF_CONN_ID,
            database=SF_STAGING_DB,
            sql=f"{STAGING_QUERY}", 
            params={
                "raw_db_name": SF_RAW_DB,
                "stg_db_name": SF_STAGING_DB,
                "schema_name": SF_SCHEMA,
                "table_name": TABLE_NAME,
                "event_dt_col": EVENT_DT_COL
            }
        )

        # Set task dependencies for each dataset
        download_task >> delete_query_task >> transform_task >> unique_query_task >> staging_query_task
        staging_tasks.append(staging_query_task)

        # TRANSFORMATION ---------------------------------------------------------------------

    orders_join_customers_reviews_task = SQLExecuteQueryOperator(
        task_id="orders_join_customers_reviews_task",
        conn_id=SF_CONN_ID,
        database=SF_RAW_DB,
        sql='sql/orders_join_customers_reviews.sql',
        params={
            "stg_db_name": SF_STAGING_DB,
            "schema_name": SF_SCHEMA
        }
    )

    order_items_join_sellers_products_task = SQLExecuteQueryOperator(
        task_id="order_items_join_sellers_products_task",
        conn_id=SF_CONN_ID,
        database=SF_RAW_DB,
        sql='sql/order_items_join_sellers_products.sql',
        params={
            "stg_db_name": SF_STAGING_DB,
            "schema_name": SF_SCHEMA
        }
    )

    orders_join_payments_task = SQLExecuteQueryOperator(
        task_id="orders_join_payments_task",
        conn_id=SF_CONN_ID,
        database=SF_RAW_DB,
        sql='sql/orders_join_payments.sql',
        params={
            "stg_db_name": SF_STAGING_DB,
            "schema_name": SF_SCHEMA
        }
    )

    staging_tasks >> orders_join_customers_reviews_task >> order_items_join_sellers_products_task >> orders_join_payments_task