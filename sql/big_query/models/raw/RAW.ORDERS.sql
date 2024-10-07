-- SQL to create the source system table of orders
CREATE TABLE IF NOT EXISTS source_dataset.source_system_order_reviews (
review_id STRING,
order_id STRING,
review_score INT,
review_comment_title STRING,
review_comment_message STRING,
review_creation_date STRING,
review_answer_timestamp STRING,
INGESTION_DATE STRING,
UPDATE_TS STRING	
)

-- SQL to create the source system table of orders
CREATE TABLE IF NOT EXISTS source_dataset.source_system_orders (
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
