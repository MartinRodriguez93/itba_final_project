DELETE FROM INTERMEDIATE.MARKETING.order_items_join_sellers_products
WHERE SHIPPING_LIMIT_dt = '{{ ds }}';

INSERT INTO INTERMEDIATE.MARKETING.order_items_join_sellers_products (
    order_item_key,
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_at,
    price,
    freight_value,
    SHIPPING_LIMIT_DT,
    updated_at,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
)
select
	stg_order_items.order_item_key, --primery key
	stg_order_items.ORDER_ID,
	stg_order_items.ORDER_ITEM_ID,
    stg_order_items.PRODUCT_ID,
	stg_order_items.SELLER_ID,
	stg_order_items.SHIPPING_LIMIT_at,
	stg_order_items.PRICE,
	stg_order_items.FREIGHT_VALUE,
    stg_order_items.SHIPPING_LIMIT_DT,
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
from {{ params.stg_db_name }}.{{ params.schema_name }}.source_system_order_items as stg_order_items
left join {{ params.stg_db_name }}.{{ params.schema_name }}.sellers as stg_sellers
    on stg_order_items.SELLER_ID = stg_sellers.SELLER_ID
left join {{ params.stg_db_name }}.{{ params.schema_name }}.products as stg_products
    on stg_order_items.product_id = stg_products.product_id
where stg_order_items.SHIPPING_LIMIT_dt = '{{ ds }}';