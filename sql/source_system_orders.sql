# SQL to create the source system table of orders
CREATE TABLE IF NOT EXISTS source_dataset.source_system_orders (
Order_ID STRING,
Order_Date STRING,
Customer_ID STRING,
Product_ID STRING,
Quantity INTEGER,
INGESTION_DATE STRING,
UPDATE_TS TIMESTAMP
)
