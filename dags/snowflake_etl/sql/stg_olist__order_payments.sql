BEGIN;

create or replace view {{ params.stg_db_name }}.{{ params.schema_name }}.{{ params.table_name }} as (
with source as (
select 
    *
from {{ params.raw_db_name }}.{{ params.schema_name }}.{{ params.table_name }}
)
--There could be more than one PAYMENT_SEQUENTIAL per ORDER_ID
, renamed as (
select
    hash(ORDER_ID, PAYMENT_SEQUENTIAL) as order_payment_key, --primery key
	ORDER_ID,
	PAYMENT_SEQUENTIAL,
	PAYMENT_TYPE,
	PAYMENT_INSTALLMENTS,
	PAYMENT_VALUE,
	PAYMENT_DATE,
    TO_TIMESTAMP(INGESTION_DATE / 1e9)::date as event_date,
    UPDATE_TS as updated_at
from source
)
select
    *
from renamed
)
;

COMMIT;