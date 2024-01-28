with 

source as (

    select * from {{ source('staging', 'source_system_orders') }}

),

renamed as (

    select
        order_id,
        order_date,
        customer_id,
        product_id,
        quantity,
        ingestion_date,
        update_ts

    from source

)

select * from renamed
