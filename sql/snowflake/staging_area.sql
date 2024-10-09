--staging source_system_order
;
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
;

--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

--staging source_system_order
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

--staging raw.orders.source_system_order_items
;
with source as (
select
    *
from raw.orders.source_system_order_items
)
--There could be more than one ORDER_ITEM_ID per ORDER_ID
, renamed as (
select
	ORDER_ID,
	ORDER_ITEM_ID,
	hash(ORDER_ID, ORDER_ITEM_ID) as order_item_id, --primery key
    PRODUCT_ID,
	SELLER_ID,
	SHIPPING_LIMIT_DATE SHIPPING_LIMIT_at,
	PRICE,
	FREIGHT_VALUE,
    TO_TIMESTAMP(INGESTION_DATE / 1e9)::date as event_date,
    UPDATE_TS as updated_at
from source
)
select
    *
from renamed
;