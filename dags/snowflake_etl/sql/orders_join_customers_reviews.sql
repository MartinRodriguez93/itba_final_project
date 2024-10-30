DELETE FROM INTERMEDIATE.MARKETING.ORDERS_JOIN_CUSTOMERS_REVIEWS
WHERE order_date = '{{ ds }}';

INSERT INTO INTERMEDIATE.MARKETING.ORDERS_JOIN_CUSTOMERS_REVIEWS (
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_STATUS,
    ORDER_PURCHASE_AT,
    ORDER_APPROVED_AT,
    ORDER_DELIVERED_CARRIER_AT,
    ORDER_DELIVERED_CUSTOMER_AT,
    ORDER_ESTIMATED_DELIVERY_AT,
    ORDER_DATE,
    CUSTOMER_UNIQUE_ID,
    CUSTOMER_ZIP_CODE_PREFIX,
    CUSTOMER_CITY,
    CUSTOMER_STATE,
    REVIEW_ID,
    REVIEW_SCORE,
    REVIEW_COMMENT_MESSAGE,
    REVIEW_CREATION_AT,
    REVIEW_ANSWER_AT,
    ORDER_REVIEW_DATE,
    UPDATED_AT
)
SELECT
    stg_orders.order_id,
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
    stg_reviews.review_id,
    stg_reviews.review_score,
    stg_reviews.review_comment_message,
    stg_reviews.review_creation_at,
    stg_reviews.review_answer_at,
    stg_reviews.event_date as order_review_date,
    stg_orders.updated_at
FROM STAGING.ORDERS.source_system_order AS stg_orders
LEFT JOIN staging.orders.customers AS stg_customers
    ON stg_orders.customer_id = stg_customers.customer_id
LEFT JOIN STAGING.ORDERS.SOURCE_SYSTEM_ORDER_REVIEWS AS stg_reviews
    ON stg_reviews.order_id = stg_orders.order_id
WHERE stg_orders.order_date = '{{ ds }}';
