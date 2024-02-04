with

    source as (select * from {{ source("olist", "source_system_orders") }}),

    stg_orders as (

        select
            --An order might have multiple items.
            --Each item might be fulfilled by a distinct seller.
            order_id,
            customer_id,
            order_status,
            TIMESTAMP (order_purchase_timestamp) AS order_purchase_ts,
            TIMESTAMP (order_approved_at) as order_approved_at_ts,
            TIMESTAMP (order_delivered_carrier_date) as order_delivered_carrier_ts,
            TIMESTAMP (order_delivered_customer_date) as order_delivered_customer_ts,
            TIMESTAMP (order_estimated_delivery_date) as order_estimated_delivery_ts,
            DATE (INGESTION_DATE) as INGESTION_dt,
            TIMESTAMP (UPDATE_TS) as UPDATE_TS

        from source

    )

select *
from stg_orders
    
