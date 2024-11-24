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
from RAW.ORDERS.sellers
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