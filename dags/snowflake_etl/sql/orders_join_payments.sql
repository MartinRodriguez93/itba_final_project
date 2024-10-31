DELETE FROM intermediate.finance.orders_join_payments
WHERE PAYMENT_DATE = '{{ ds }}';

INSERT INTO intermediate.finance.orders_join_payments (
    order_payment_key,
    ORDER_ID,
    PAYMENT_SEQUENTIAL,
    PAYMENT_TYPE,
    PAYMENT_INSTALLMENTS,
    PAYMENT_VALUE,
    PAYMENT_DATE,
    order_purchase_at,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
)
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
from {{ params.stg_db_name }}.{{ params.schema_name }}.SOURCE_SYSTEM_ORDER_PAYMENTS as stg_payments
left join intermediate.marketing.ORDERS_JOIN_CUSTOMERS_REVIEWS as int_orders_customers
    on stg_payments.order_id = int_orders_customers.order_id
where stg_payments.PAYMENT_DATE = '{{ ds }}';