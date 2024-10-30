DELETE FROM {{ params.raw_db_name }}.{{ params.schema_name }}.{{ params.table_name }}
WHERE {{ params.event_dt_col }}::date = '{{ ds }}';
