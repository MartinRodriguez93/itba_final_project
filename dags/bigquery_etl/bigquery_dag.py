import os
from pendulum import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyTableOperator
)

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

from dags.bigquery_etl.functions.bigquery_dag_functions import (
    transform_source_system
)

# VARIABLES DEFINITION ----------------------------------------------------------------------------------------

# Env variables
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
LOCATION = os.environ.get("LOCATION", 'US')
BQ_CONN_ID = "gcp"

# Common variables
RUN_DS = '{{ ds }}'
TEMPLATE_SEARCHPATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")
BIGQUERY_DB = 'itbafinalproject'
RAW_SCHEMA = "RAW"
STAGING_SCHEMA = 'staging'

# Unique variables
DAG_DICT = {
    "olist_orders_dataset": [
        'olist_orders_dataset.csv',
        'olist_orders_{{ ds }}.csv',
        'source_system_order',
        'order_purchase_timestamp',
        """
        CREATE OR REPLACE VIEW `{bigquery_db}.{stg_schema_name}.{table_name}` AS
        SELECT
            order_id, -- primary key
            customer_id,
            order_status,
            order_purchase_timestamp AS order_purchase_at,
            DATE(order_purchase_timestamp) AS order_dt,
            order_approved_at,
            order_delivered_carrier_date AS order_delivered_carrier_at,
            order_delivered_customer_date AS order_delivered_customer_at,
            order_estimated_delivery_date AS order_estimated_delivery_at,
            UPDATE_TS AS updated_at
        FROM
            `{bigquery_db}.{raw_schema_name}.{table_name}`
        """
        ],
    "olist_order_reviews_dataset": [
        'olist_order_reviews_dataset.csv', 
        'olist_order_reviews_{{ ds }}.csv', 
        'source_system_order_reviews', 
        'review_creation_date',
        """
        create or replace view `{bigquery_db}.{stg_schema_name}.{table_name}` as (
        with source as (
        select 
            *
        from `{bigquery_db}.{raw_schema_name}.{table_name}`
        )
        --There could be more than one order_id per review_id
        , renamed AS (
            SELECT
                FARM_FINGERPRINT(CONCAT(CAST(review_id AS STRING), CAST(order_id AS STRING))) AS review_key, -- Primary key
                review_id, -- Primary key
                order_id,
                review_score, -- Values between 1 and 5
                review_comment_title, -- Always null
                review_comment_message,
                review_creation_date AS review_creation_at,
                DATE(review_creation_date) AS review_creation_dt,
                review_answer_timestamp AS review_answer_at,
                UPDATE_TS AS updated_at
            FROM source
        )
        SELECT
            *
        FROM renamed
    )
        """
        ],
    "olist_order_items_dataset": [
        'olist_order_items_dataset.csv', 
        'olist_order_items_{{ ds }}.csv', 
        'source_system_order_items', 
        'shipping_limit_date',
        """
        CREATE OR REPLACE VIEW `{bigquery_db}.{stg_schema_name}.{table_name}` AS (
            WITH source AS (
                SELECT 
                    *
                FROM `{bigquery_db}.{raw_schema_name}.{table_name}`
            ),
            renamed AS (
                SELECT
                    FARM_FINGERPRINT(CONCAT(ORDER_ID, ORDER_ITEM_ID)) AS order_item_key, -- primary key
                    ORDER_ID,
                    ORDER_ITEM_ID,
                    PRODUCT_ID,
                    SELLER_ID,
                    SHIPPING_LIMIT_DATE AS SHIPPING_LIMIT_at,
                    DATE(SHIPPING_LIMIT_DATE) AS SHIPPING_LIMIT_dt,
                    PRICE,
                    FREIGHT_VALUE,
                    UPDATE_TS AS updated_at
                FROM source
            )
            SELECT
                *
            FROM renamed
        )
       """
        ],
    "olist_order_payments_dataset": [
        'olist_order_payments_dataset.csv', 
        'olist_order_payments{{ ds }}.csv', 
        'source_system_order_payments', 
        'PAYMENT_DATE',
       """
        CREATE OR REPLACE VIEW `{bigquery_db}.{stg_schema_name}.{table_name}` AS (
        WITH source AS (
            SELECT 
                *
            FROM `{bigquery_db}.{raw_schema_name}.{table_name}`
        )
        -- There could be more than one PAYMENT_SEQUENTIAL per ORDER_ID
        , renamed AS (
            SELECT
                FARM_FINGERPRINT(CONCAT(ORDER_ID, CAST(PAYMENT_SEQUENTIAL AS STRING))) AS order_payment_key, -- primary key
                ORDER_ID,
                PAYMENT_SEQUENTIAL,
                PAYMENT_TYPE,
                PAYMENT_INSTALLMENTS,
                PAYMENT_VALUE,
                PAYMENT_DATE,
                UPDATE_TS AS updated_at
            FROM source
        )
        SELECT
            *
        FROM renamed
        )
            """
        ]
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

with DAG('bigquery_dag', default_args=DEFAULT_ARGS, template_searchpath=[TEMPLATE_SEARCHPATH]) as dag:

    staging_tasks = []
    
    for dataset, params in DAG_DICT.items():
        DATASET_SOURCE, DATASET_FILE, TABLE_NAME, EVENT_DT_COL, STAGING_QUERY_TEMPLATE = params

        # GENERATION ---------------------------------------------------------------------

        download_task = BashOperator(
            task_id=f"download_{dataset}",
            bash_command=f"bash {PATH_TO_LOCAL_HOME}/dags/scripts/download_dataset.sh {PATH_TO_LOCAL_HOME} {DATASET_SOURCE}"
        )

        # INGESTION ---------------------------------------------------------------------

        transform_task = PythonOperator(
            task_id=f"transform_{dataset}",
            python_callable=transform_source_system,
            op_kwargs={
                "src_file": f"{PATH_TO_LOCAL_HOME}/{DATASET_SOURCE}",
                "file_name": DATASET_FILE,
                "ingestion_date": RUN_DS,
                "event_dt_col": EVENT_DT_COL
            },
        )

        # UPLOAD TO BUCKET

        local_to_gcs_task = LocalFilesystemToGCSOperator(
            task_id=f"local_to_gcs_{dataset}",
            src=f"{PATH_TO_LOCAL_HOME}/{DATASET_FILE}",
            dst=f"raw/{DATASET_FILE}",
            bucket=BUCKET,
            gcp_conn_id='gcp',
            mime_type='text/csv',
        )

        DELETE_QUERY = (
            f"DELETE {RAW_SCHEMA}.{TABLE_NAME} \
            WHERE INGESTION_DATE = '{RUN_DS}';"
        )

        delete_query_task = BigQueryInsertJobOperator(
            task_id=f"delete_query_{dataset}",
            configuration={
                "query": {
                    "query": DELETE_QUERY,
                    "useLegacySql": False,
                    "priority": "BATCH",
                }
            },
            location=LOCATION,
            deferrable=True
        )

        append_rows_task = GoogleCloudStorageToBigQueryOperator(
            #gcp conn defined in env AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT & GOOGLE_APPLICATION_CREDENTIALS
            task_id=f"append_rows_{dataset}",
            bucket=BUCKET,
            source_objects=[f"raw/{DATASET_FILE}"],
            destination_project_dataset_table=f"{RAW_SCHEMA}.{TABLE_NAME}",
            schema_object=f"source_systems_schemas/{TABLE_NAME}_schema.json", #BUCKET FOLDER
            source_format='CSV',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_APPEND',
            field_delimiter=',',
            skip_leading_rows=1,
            allow_quoted_newlines=True, #Parse csv cols with comments
            autodetect=False
        )

        # Render the query with Python string formatting
        STAGING_QUERY = STAGING_QUERY_TEMPLATE.format(
            bigquery_db = BIGQUERY_DB,
            raw_schema_name=RAW_SCHEMA,
            stg_schema_name= STAGING_SCHEMA,
            table_name=TABLE_NAME,
        )

        staging_query_task = BigQueryInsertJobOperator(
            task_id=f"staging_query_{dataset}",
            configuration={
                "query": {
                    "query": STAGING_QUERY,
                    "useLegacySql": False,
                    "priority": "BATCH",
                }
            },
            location=LOCATION,
        )

        # Set task dependencies for each dataset
        download_task >> transform_task >> local_to_gcs_task >> delete_query_task >> append_rows_task >> staging_query_task
        staging_tasks.append(staging_query_task)

    # TRANSFORMATION ---------------------------------------------------------------------

    orders_join_customers_reviews_QUERY_TEMPLATE = """
    DELETE FROM itbafinalproject.marketing.ORDERS_JOIN_CUSTOMERS_REVIEWS
    WHERE order_dt = '{ds}';

    INSERT INTO itbafinalproject.marketing.ORDERS_JOIN_CUSTOMERS_REVIEWS (
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_STATUS,
        ORDER_PURCHASE_AT,
        ORDER_APPROVED_AT,
        ORDER_DELIVERED_CARRIER_AT,
        ORDER_DELIVERED_CUSTOMER_AT,
        ORDER_ESTIMATED_DELIVERY_AT,
        ORDER_DT,
        CUSTOMER_UNIQUE_ID,
        CUSTOMER_ZIP_CODE_PREFIX,
        CUSTOMER_CITY,
        CUSTOMER_STATE,
        REVIEW_ID,
        REVIEW_SCORE,
        REVIEW_COMMENT_MESSAGE,
        REVIEW_CREATION_AT,
        REVIEW_ANSWER_AT,
        review_creation_dt,
        UPDATED_AT
    )
    SELECT
        stg_orders.order_id,
        stg_orders.customer_id,
        stg_orders.order_status,
        stg_orders.order_purchase_at,
        stg_orders.order_approved_at,
        stg_orders.order_delivered_carrier_at,
        stg_orders.order_delivered_customer_at,
        stg_orders.order_estimated_delivery_at,
        stg_orders.order_dt,
        stg_customers.customer_unique_id,
        stg_customers.customer_zip_code_prefix,
        stg_customers.customer_city,
        stg_customers.customer_state,
        stg_reviews.review_id,
        stg_reviews.review_score,
        stg_reviews.review_comment_message,
        stg_reviews.review_creation_at,
        stg_reviews.review_answer_at,
        stg_reviews.review_creation_dt,
        stg_orders.updated_at
    FROM {bigquery_db}.{stg_schema_name}.source_system_order AS stg_orders
    LEFT JOIN {bigquery_db}.{stg_schema_name}.customers AS stg_customers
        ON stg_orders.customer_id = stg_customers.customer_id
    LEFT JOIN {bigquery_db}.{stg_schema_name}.source_system_order_reviews AS stg_reviews
        ON stg_reviews.order_id = stg_orders.order_id
    WHERE stg_orders.order_dt = '{ds}';
    """
    # Render the query with Python string formatting
    orders_join_customers_reviews_QUERY = orders_join_customers_reviews_QUERY_TEMPLATE.format(
        bigquery_db = BIGQUERY_DB,
        raw_schema_name = RAW_SCHEMA,
        stg_schema_name = STAGING_SCHEMA,
        ds = RUN_DS
    )

    orders_join_customers_reviews_task = BigQueryInsertJobOperator(
        task_id="orders_join_customers_reviews_task",
        configuration={
            "query": {
                "query": orders_join_customers_reviews_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
    )

    # order_items_join_sellers_products_task = SQLExecuteQueryOperator(
    #     task_id="order_items_join_sellers_products_task",
    #     conn_id=SF_CONN_ID,
    #     database=SF_RAW_DB,
    #     sql='sql/order_items_join_sellers_products.sql',
    #     params={
    #         "stg_db_name": SF_STAGING_DB,
    #         "schema_name": SF_SCHEMA
    #     }
    # )

    # orders_join_payments_task = SQLExecuteQueryOperator(
    #     task_id="orders_join_payments_task",
    #     conn_id=SF_CONN_ID,
    #     database=SF_RAW_DB,
    #     sql='sql/orders_join_payments.sql',
    #     params={
    #         "stg_db_name": SF_STAGING_DB,
    #         "schema_name": SF_SCHEMA
    #     }
    # )


    staging_tasks >> orders_join_customers_reviews_task
    # staging_tasks >> orders_join_customers_reviews_task >> order_items_join_sellers_products_task >> orders_join_payments_task