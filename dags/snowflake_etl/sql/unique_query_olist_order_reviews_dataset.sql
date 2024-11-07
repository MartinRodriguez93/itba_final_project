select
    review_id,
    order_id,
    count(*) as unique_test
from {{ params.raw_db_name }}.{{ params.schema_name }}.{{ params.table_name }}
where {{ params.event_dt_col }}::date = '{{ ds }}'
group by 1,2
having unique_test > 1
;