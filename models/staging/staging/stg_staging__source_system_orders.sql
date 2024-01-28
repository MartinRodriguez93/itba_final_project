with

    source as (select * from {{ source("staging", "source_system_orders") }}),

    stg_orders as (

        select
            order_id,
            PARSE_DATE('%m/%d/%Y',  order_date) as order_date,
            customer_id,
            product_id,
            quantity,
            PARSE_DATE('%F', ingestion_date) AS ingested_at,
            update_ts

        from source

    )

select *
from stg_orders
where
    1 = 1
    -- and order_date = ''
    
