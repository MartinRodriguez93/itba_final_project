CREATE TABLE `itbafinalproject.RAW.source_system_order_reviews` (
  review_id STRING,
  order_id STRING,
  review_score INT64,
  review_comment_title STRING,
  review_comment_message STRING,
  review_creation_date STRING,
  review_answer_timestamp STRING,
  INGESTION_DATE STRING,
  UPDATE_TS STRING
);

-- SQL to create the source system table of orders
CREATE TABLE IF NOT EXISTS RAW.ORDERS.SOURCE_SYSTEM_ORDER (
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

CREATE or replace TABLE `itbafinalproject.RAW.source_system_order_items` (
    ORDER_ID STRING,
    ORDER_ITEM_ID INT64,
    PRODUCT_ID STRING,
    SELLER_ID STRING,
    SHIPPING_LIMIT_DATE TIMESTAMP,
    PRICE FLOAT64,
    FREIGHT_VALUE FLOAT64,
    INGESTION_DATE STRING,
    UPDATE_TS TIMESTAMP
);

CREATE OR REPLACE TABLE `itbafinalproject.RAW.source_system_order_payments` (
    ORDER_ID STRING,
    PAYMENT_SEQUENTIAL INT64,
    PAYMENT_TYPE STRING,
    PAYMENT_INSTALLMENTS INT64,
    PAYMENT_VALUE FLOAT64,
    PAYMENT_DATE DATE,
    INGESTION_DATE STRING,
    UPDATE_TS TIMESTAMP
);