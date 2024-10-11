-- stg_olist__orders.sql
;
create or replace view staging.orders.orders as (
with source as (
select 
    *
from raw.orders.source_system_order
)
--An order might have multiple items.
--Each item might be fulfilled by a distinct seller.
, renamed as (
select
    order_id, --primery key
    customer_id,
    order_status,
    order_purchase_timestamp AS order_purchase_at,
    order_approved_at,
    order_delivered_carrier_date as order_delivered_carrier_at,
    order_delivered_customer_date as order_delivered_customer_at,
    order_estimated_delivery_date as order_estimated_delivery_at,
    TO_TIMESTAMP(INGESTION_DATE / 1e9)::date as event_date,
    UPDATE_TS as updated_at
from source
)
select
    *
from renamed
)
;

--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

-- stg_olist__order_reviews.sql
;
with source as (
select
    *
from raw.orders.source_system_order_reviews
)
--There could be more than one order_id per review_id
--Referential integrity with source_system_orders fails because of cut off time of date.
--Ex: 0764f8b9a45b0305b75e1bbd614ffaaa dated 2017-09-16 
, renamed as (
select
    review_id, --primery key
    order_id,
    review_score, --Values between 1 and 5
    review_comment_title, --always null
    review_comment_message,
    review_creation_date AS review_creation_at,
    review_answer_timestamp AS review_answer_at,
    TO_TIMESTAMP(INGESTION_DATE / 1e9)::date as event_date,
    UPDATE_TS as updated_at
from source
)
select
    *
from renamed
;

--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

-- stg_olist__order_items.sql
;
create or replace view staging.orders.order_items as (
with source as (
select
    *
from raw.orders.source_system_order_items
)
--There could be more than one ORDER_ITEM_ID per ORDER_ID
, renamed as (
select
	hash(ORDER_ID, ORDER_ITEM_ID) as order_item_key, --primery key
	ORDER_ID,
	ORDER_ITEM_ID,
    PRODUCT_ID,
	SELLER_ID,
	SHIPPING_LIMIT_DATE as SHIPPING_LIMIT_at,
	PRICE,
	FREIGHT_VALUE,
    TO_TIMESTAMP(INGESTION_DATE / 1e9)::date as event_date,
    UPDATE_TS as updated_at
from source
)
select
    *
from renamed
)
;

--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

-- stg_olist__order_payments.sql
;
create or replace view staging.orders.payments as (
with source as (
select
    *
from raw.orders.SOURCE_SYSTEM_ORDER_PAYMENTS
)
--There could be more than one PAYMENT_SEQUENTIAL per ORDER_ID
, renamed as (
select
    hash(ORDER_ID, PAYMENT_SEQUENTIAL) as order_payment_key, --primery key
	ORDER_ID,
	PAYMENT_SEQUENTIAL,
	PAYMENT_TYPE,
	PAYMENT_INSTALLMENTS,
	PAYMENT_VALUE,
	PAYMENT_DATE,
    TO_TIMESTAMP(INGESTION_DATE / 1e9)::date as event_date,
    UPDATE_TS as updated_at
from source
)
select
    *
from renamed
)
;

--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

-- stg_olist__customers.sql
;
create or replace view staging.orders.customers as (
with source as (
select
    *
from RAW.ORDERS.customer_data
)
-- one customer can exist in more than one city or state
, renamed as (
select
    customer_id, --primary key
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
from source
)
select
    *
from renamed
)
;

--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

-- stg_olist__products.sql
;
create or replace view staging.orders.products as (
with source as (
select
    *
from RAW.ORDERS.product_data
)
, renamed as (
select
    product_id, --primary key
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
from source
)
select
    *
from renamed
)
;

--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

-- stg_olist__sellers.sql
;
create or replace view staging.orders.sellers as (
with source as (
select
    *
from RAW.ORDERS.seller_data
)
, renamed as (
select
    seller_id, --primary key
    seller_zip_code_prefix,
    seller_city,
    seller_state
from source
)
select
    *
from renamed
)
;

--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

-- stg_olist__products_category_translation.sql
;
with source as (
select
    *
from RAW.ORDERS.product_category_translation
)
, renamed as (
select
    product_category_name,
    product_category_name_english
from source
)
select
    *
from renamed
;