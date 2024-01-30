with

    source as (select * from {{ source("coffee_shop", "source_system_orders") }}),

    stg_orders as (

        select
            --It's not unique, because each order_id can appear more than once if more than one product_id was ordered
            order_id,
            PARSE_DATE('%m/%d/%Y',  order_date) as order_date,
            customer_id,
            --product_id can be repeated for the same order_id
            product_id,
            quantity,
            PARSE_DATE('%F', ingestion_date) AS ingested_at,
            update_ts,
            'test' AS test_ci

        from source

    )

select *
from stg_orders
    
