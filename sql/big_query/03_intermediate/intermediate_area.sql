CREATE OR REPLACE TABLE INTERMEDIATE.MARKETING.ORDERS_JOIN_CUSTOMERS_REVIEWS (
    ORDER_ID STRING,
    CUSTOMER_ID STRING,
    ORDER_STATUS STRING,
    ORDER_PURCHASE_AT TIMESTAMP_NTZ,
    ORDER_APPROVED_AT TIMESTAMP_NTZ,
    ORDER_DELIVERED_CARRIER_AT TIMESTAMP_NTZ,
    ORDER_DELIVERED_CUSTOMER_AT TIMESTAMP_NTZ,
    ORDER_ESTIMATED_DELIVERY_AT TIMESTAMP_NTZ,
    ORDER_DT DATE,
    CUSTOMER_UNIQUE_ID STRING,
    CUSTOMER_ZIP_CODE_PREFIX STRING,
    CUSTOMER_CITY STRING,
    CUSTOMER_STATE STRING,
    REVIEW_ID STRING,
    REVIEW_SCORE NUMBER(1,0),
    REVIEW_COMMENT_MESSAGE STRING,
    REVIEW_CREATION_AT TIMESTAMP_NTZ,
    REVIEW_ANSWER_AT TIMESTAMP_NTZ,
    review_creation_dt DATE,
    UPDATED_AT TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE INTERMEDIATE.MARKETING.order_items_join_sellers_products (
    order_item_key STRING,
    order_id STRING,
    order_item_id STRING,
    product_id STRING,
    seller_id STRING,
    shipping_limit_at TIMESTAMP,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2),
    SHIPPING_LIMIT_DT DATE,
    updated_at TIMESTAMP,
    
    seller_zip_code_prefix STRING,
    seller_city STRING,
    seller_state STRING,

    product_category_name STRING,
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

;
create or replace view intermediate.finance.orders_join_payments as (
select
    stg_payments.order_payment_key,
	stg_payments.ORDER_ID,
	stg_payments.PAYMENT_SEQUENTIAL,
	stg_payments.PAYMENT_TYPE,
	stg_payments.PAYMENT_INSTALLMENTS,
	stg_payments.PAYMENT_VALUE,
	stg_payments.PAYMENT_DATE,
    int_orders_customers.order_purchase_at,
    int_orders_customers.customer_unique_id,
    int_orders_customers.customer_zip_code_prefix,
    int_orders_customers.customer_city,
    int_orders_customers.customer_state
from staging.orders.payments as stg_payments
left join intermediate.marketing.orders_join_customers as int_orders_customers
    on stg_payments.order_id = int_orders_customers.order_id
)
;