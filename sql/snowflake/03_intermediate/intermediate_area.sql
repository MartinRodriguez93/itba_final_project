--int_marketing_orders_join_customers
;
create or replace view intermediate.marketing.orders_join_customers as (
select
    stg_orders.order_id, --primery key
    stg_orders.customer_id,
    stg_orders.order_status,
    stg_orders.order_purchase_at,
    stg_orders.order_approved_at,
    stg_orders.order_delivered_carrier_at,
    stg_orders.order_delivered_customer_at,
    stg_orders.order_estimated_delivery_at,
    stg_orders.event_date,
    stg_orders.updated_at,
    stg_customers.customer_unique_id,
    stg_customers.customer_zip_code_prefix,
    stg_customers.customer_city,
    stg_customers.customer_state
from staging.orders.orders as stg_orders
left join staging.orders.customers as stg_customers
    on stg_orders.customer_id = stg_customers.customer_id
)
;

--int_marketing_order_items_join_sellers
;
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
    stg_sellers.seller_state
from staging.orders.order_items as stg_order_items
left join staging.orders.sellers as stg_sellers
    on stg_order_items.SELLER_ID = stg_sellers.SELLER_ID
;

--int_finance_orders_join_payments
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

--int_marketing_order_items_join_products
;
create or replace view intermediate.marketing.order_items_join_products as (
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
left join staging.orders.products as stg_products
    on stg_order_items.product_id = stg_products.product_id
)
;