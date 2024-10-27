import os
from pendulum import datetime
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

# VARIABLES DEFINITION ----------------------------------------------------------------------------------------

#Env variables
path_to_local_home = os.environ.get("AIRFLOW_HOME", "")
SF_USER = os.environ.get("SF_USER")
SF_ACCOUNT = os.environ.get("SF_ACCOUNT")
SF_PWD = os.environ.get("SF_PWD")
_SNOWFLAKE_CONN_ID = "snowflake_conn"

#Common variables
_SNOWFLAKE_DB = "RAW"
_SNOWFLAKE_SCHEMA = "ORDERS"

# DAG DEFINITION -------------------------------------------------------------------------------------

DAG_ID = 'snowflake_transform_dag'

# Define the default arguments
default_args = {
    "owner": "snowflake",
    "start_date": datetime(2017, 10, 2),
    "end_date": datetime(2017, 10, 6),
    "schedule": "@daily",
    "depends_on_past": True, #simulate daily load
    "catchup": True, #simulate daily load
    "max_active_runs": 1,
    "retries": 1
}

# Define the list of external DAG dependencies
external_dags = [
    {"dag_id": "snowflake_olist_orders_dataset", "task_id": "transform_source_system_task"},
    {"dag_id": "snowflake_olist_order_reviews_dataset", "task_id": "transform_source_system_task"},
    {"dag_id": "snowflake_olist_order_items_dataset", "task_id": "transform_source_system_task"},
    {"dag_id": "snowflake_olist_order_payments_dataset", "task_id": "transform_source_system_task"},
]

with DAG(DAG_ID, default_args=default_args) as dag:

    # Create wait_for_dag tasks dynamically
    wait_for_dag_tasks = []
    for external_dag in external_dags:
        wait_task = ExternalTaskSensor(
            task_id=f'wait_for_{external_dag["dag_id"]}',  # Task ID based on dag_id
            external_dag_id=external_dag["dag_id"],
            external_task_id=external_dag["task_id"],
            timeout=600,  # Time (in seconds) to wait for completion before failing
            mode='poke',  # Options are 'poke' or 'reschedule'
        )
        wait_for_dag_tasks.append(wait_task)

    # TRANSFORMATION ---------------------------------------------------------------------

    orders_join_customers_reviews_task = SQLExecuteQueryOperator(
        task_id="orders_join_customers_reviews_task",
        conn_id=_SNOWFLAKE_CONN_ID,
        database=_SNOWFLAKE_DB,
        sql='sql/orders_join_customers_reviews.sql',
        params={
            "db_name": _SNOWFLAKE_DB,
            "schema_name": _SNOWFLAKE_SCHEMA
        }
    )

    wait_for_dag_tasks >> orders_join_customers_reviews_task
