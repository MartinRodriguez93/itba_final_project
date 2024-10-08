DELETE FROM {{ params.db_name }}.{{ params.schema_name }}.{{ params.table_name }}
WHERE TO_TIMESTAMP(INGESTION_DATE / 1e9) = '{{ ds }}';