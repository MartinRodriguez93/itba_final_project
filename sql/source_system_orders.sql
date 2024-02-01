# SQL to create the source system table of orders
CREATE TABLE IF NOT EXISTS source_dataset.source_system_orders (
index STRING,
order_id STRING,
customer_id STRING,
order_status STRING,
order_purchase_timestamp STRING,
order_approved_at STRING,
order_delivered_carrier_date STRING,
order_delivered_customer_date STRING,
order_estimated_delivery_date STRING,
INGESTION_DATE STRING,
UPDATE_TS STRING	
)
