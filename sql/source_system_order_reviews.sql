# SQL to create the source system table of orders
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
