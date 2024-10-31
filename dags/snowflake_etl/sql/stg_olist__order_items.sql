BEGIN;

create or replace view {{ params.stg_db_name }}.{{ params.schema_name }}.{{ params.table_name }} as (
with source as (
select 
    *
from {{ params.raw_db_name }}.{{ params.schema_name }}.{{ params.table_name }}
)
--There could be more than one ORDER_ITEM_ID per ORDER_ID
, renamed as (
select
	hash(ORDER_ID, ORDER_ITEM_ID) as order_item_key, --primery key
	ORDER_ID,
	ORDER_ITEM_ID,
    PRODUCT_ID,
	SELLER_ID,
	SHIPPING_LIMIT_DATE as SHIPPING_LIMIT_at,
	SHIPPING_LIMIT_DATE::date as SHIPPING_LIMIT_dt,
	PRICE,
	FREIGHT_VALUE,
    UPDATE_TS as updated_at
from source
)
select
    *
from renamed
)
;

COMMIT;