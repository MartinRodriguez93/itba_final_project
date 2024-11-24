BEGIN;

create or replace view {{ params.stg_db_name }}.{{ params.schema_name }}.{{ params.table_name }} as (
with source as (
select 
    *
from {{ params.raw_db_name }}.{{ params.schema_name }}.{{ params.table_name }}
)
--There could be more than one order_id per review_id
, renamed as (
select
    hash(review_id, order_id) as review_key, --primery key
    review_id, --primery key
    order_id,
    review_score, --Values between 1 and 5
    review_comment_title, --always null
    review_comment_message,
    review_creation_date AS review_creation_at,
    review_creation_date::date AS review_creation_dt,
    review_answer_timestamp AS review_answer_at,
    UPDATE_TS as updated_at
from source
)
select
    *
from renamed
)
;

COMMIT;