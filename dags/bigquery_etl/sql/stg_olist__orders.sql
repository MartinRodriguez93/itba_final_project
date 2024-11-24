BEGIN;

create or replace view {{ params.stg_db_name }}.{{ params.schema_name }}.{{ params.table_name }} as (
with source as (
select 
    *
from {{ params.raw_db_name }}.{{ params.schema_name }}.{{ params.table_name }}
)
--An order might have multiple items.
--Each item might be fulfilled by a distinct seller.
, renamed as (
select
    order_id, --primery key
    customer_id,
    order_status,
    order_purchase_timestamp AS order_purchase_at,
    order_purchase_timestamp::date as order_dt,
    order_approved_at,
    order_delivered_carrier_date as order_delivered_carrier_at,
    order_delivered_customer_date as order_delivered_customer_at,
    order_estimated_delivery_date as order_estimated_delivery_at,
    UPDATE_TS as updated_at
from source
)
select
    *
from renamed
)
;

COMMIT;