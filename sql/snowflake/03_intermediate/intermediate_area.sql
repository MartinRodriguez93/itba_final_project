CREATE OR REPLACE TABLE INTERMEDIATE.MARKETING.ORDERS_JOIN_CUSTOMERS_REVIEWS (
    ORDER_ID STRING,
    CUSTOMER_ID STRING,
    ORDER_STATUS STRING,
    ORDER_PURCHASE_AT TIMESTAMP_NTZ,
    ORDER_APPROVED_AT TIMESTAMP_NTZ,
    ORDER_DELIVERED_CARRIER_AT TIMESTAMP_NTZ,
    ORDER_DELIVERED_CUSTOMER_AT TIMESTAMP_NTZ,
    ORDER_ESTIMATED_DELIVERY_AT TIMESTAMP_NTZ,
    ORDER_DATE DATE,
    CUSTOMER_UNIQUE_ID STRING,
    CUSTOMER_ZIP_CODE_PREFIX STRING,
    CUSTOMER_CITY STRING,
    CUSTOMER_STATE STRING,
    REVIEW_ID STRING,
    REVIEW_SCORE NUMBER(1,0),
    REVIEW_COMMENT_MESSAGE STRING,
    REVIEW_CREATION_AT TIMESTAMP_NTZ,
    REVIEW_ANSWER_AT TIMESTAMP_NTZ,
    ORDER_REVIEW_DATE DATE,
    UPDATED_AT TIMESTAMP_NTZ
);

;
create or replace view intermediate.marketing.orders_join_customers_reviews as (
select
    stg_orders.order_id, --primery key
    stg_orders.customer_id,
    stg_orders.order_status,
    stg_orders.order_purchase_at,
    stg_orders.order_approved_at,
    stg_orders.order_delivered_carrier_at,
    stg_orders.order_delivered_customer_at,
    stg_orders.order_estimated_delivery_at,
    stg_orders.order_date,
    
    stg_customers.customer_unique_id,
    stg_customers.customer_zip_code_prefix,
    stg_customers.customer_city,
    stg_customers.customer_state,

    --Referential integrity with source_system_orders fails because of cut off time of date.
    --Ex: 0764f8b9a45b0305b75e1bbd614ffaaa dated 2017-09-16
    stg_reviews.review_id,
    stg_reviews.review_score, --Values between 1 and 5
    stg_reviews.review_comment_message,
    stg_reviews.review_creation_at,
    stg_reviews.review_answer_at,
    stg_reviews.event_date as order_review_date,

    stg_orders.updated_at
from staging.orders.orders as stg_orders
left join staging.orders.customers as stg_customers
    on stg_orders.customer_id = stg_customers.customer_id
left join staging.orders.order_reviews as stg_reviews
    on stg_reviews.order_id = stg_orders.order_id
)
;

;
create or replace view intermediate.marketing.order_items_join_sellers_products as (
select
	stg_order_items.order_item_key, --primery key
	stg_order_items.ORDER_ID,
	stg_order_items.ORDER_ITEM_ID,
    stg_order_items.PRODUCT_ID,
	stg_order_items.SELLER_ID,
	stg_order_items.SHIPPING_LIMIT_at,
	stg_order_items.PRICE,
	stg_order_items.FREIGHT_VALUE,
    stg_order_items.event_date,
    stg_order_items.updated_at,
    
    stg_sellers.seller_zip_code_prefix,
    stg_sellers.seller_city,
    stg_sellers.seller_state,

    --there are products without category associated
    coalesce(stg_products.product_category_name,'no_data') as product_category_name,
    stg_products.product_name_length,
    stg_products.product_description_length,
    stg_products.product_photos_qty,
    stg_products.product_weight_g,
    stg_products.product_length_cm,
    stg_products.product_height_cm,
    stg_products.product_width_cm
from staging.orders.order_items as stg_order_items
left join staging.orders.sellers as stg_sellers
    on stg_order_items.SELLER_ID = stg_sellers.SELLER_ID
left join staging.orders.products as stg_products
    on stg_order_items.product_id = stg_products.product_id
)
;

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