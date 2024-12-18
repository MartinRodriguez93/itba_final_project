create or replace TABLE RAW.ORDERS.SOURCE_SYSTEM_ORDER (
	ORDER_ID VARCHAR(16777216),
	CUSTOMER_ID VARCHAR(16777216),
	ORDER_STATUS VARCHAR(16777216),
	ORDER_PURCHASE_TIMESTAMP VARCHAR(16777216),
	ORDER_APPROVED_AT VARCHAR(16777216),
	ORDER_DELIVERED_CARRIER_DATE VARCHAR(16777216),
	ORDER_DELIVERED_CUSTOMER_DATE VARCHAR(16777216),
	ORDER_ESTIMATED_DELIVERY_DATE VARCHAR(16777216),
	INGESTION_DATE NUMBER(38,0),
	UPDATE_TS TIMESTAMP_NTZ(9)
);

;
create or replace TABLE RAW.ORDERS.SOURCE_SYSTEM_ORDER_REVIEWS (
	REVIEW_ID VARCHAR(16777216),
	ORDER_ID VARCHAR(16777216),
	REVIEW_SCORE NUMBER(38,0),
	REVIEW_COMMENT_TITLE NUMBER(38,0),
	REVIEW_COMMENT_MESSAGE VARCHAR(16777216),
	REVIEW_CREATION_DATE VARCHAR(16777216),
	REVIEW_ANSWER_TIMESTAMP VARCHAR(16777216),
	INGESTION_DATE NUMBER(38,0),
	UPDATE_TS TIMESTAMP_NTZ(9)
);

create or replace TABLE RAW.ORDERS.SOURCE_SYSTEM_ORDER_ITEMS (
	ORDER_ID VARCHAR(16777216),
	ORDER_ITEM_ID NUMBER(38,0),
	PRODUCT_ID VARCHAR(16777216),
	SELLER_ID VARCHAR(16777216),
	SHIPPING_LIMIT_DATE TIMESTAMP_NTZ(9),
	PRICE FLOAT,
	FREIGHT_VALUE FLOAT,
	INGESTION_DATE NUMBER(38,0),
	UPDATE_TS TIMESTAMP_NTZ(9)
);

create or replace TABLE RAW.ORDERS.SOURCE_SYSTEM_ORDER_PAYMENTS (
	ORDER_ID VARCHAR(16777216),
	PAYMENT_SEQUENTIAL NUMBER(38,0),
	PAYMENT_TYPE VARCHAR(16777216),
	PAYMENT_INSTALLMENTS NUMBER(38,0),
	PAYMENT_VALUE FLOAT,
	PAYMENT_DATE DATE,
	INGESTION_DATE NUMBER(38,0),
	UPDATE_TS TIMESTAMP_NTZ(9)
);

CREATE TABLE RAW.ORDERS.customer_data (
    customer_id VARCHAR(255),
    customer_unique_id VARCHAR(255),
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(255),
    customer_state VARCHAR(2)
);

CREATE TABLE RAW.ORDERS.product_data (
    product_id VARCHAR(255),
    product_category_name VARCHAR(255),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

CREATE TABLE RAW.ORDERS.product_category_translation (
    product_category_name VARCHAR(255),
    product_category_name_english VARCHAR(255)
);

CREATE TABLE RAW.ORDERS.sellers (
    seller_id STRING,
    seller_zip_code_prefix INTEGER,
    seller_city STRING,
    seller_state STRING
);
