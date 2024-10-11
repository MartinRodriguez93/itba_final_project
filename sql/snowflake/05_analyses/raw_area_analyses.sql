;
select
    --768 704 1,081 812
    -- *

    -- 2017-10-02	143 35 230 157
    -- 2017-10-03	198 161 209 205
    -- 2017-10-04	157 210 203 168
    -- 2017-10-05	140 144 312 144
    -- 2017-10-06	130 154	127 138
    TO_TIMESTAMP(INGESTION_DATE / 1e9)::date as INGESTION_TIMESTAMP,
    count(*)
    
    -- distinct
    -- update_ts::date

    --768 699
    -- distinct
    -- order_id
    -- review_id
-- from raw.orders.source_system_order
-- from RAW.ORDERS.source_system_order_reviews
-- from raw.orders.source_system_order_items
from raw.orders.SOURCE_SYSTEM_ORDER_PAYMENTS

-- from RAW.ORDERS.customer_data
-- from RAW.ORDERS.product_data
-- from RAW.ORDERS.seller_data
-- from RAW.ORDERS.product_category_translation
group by 1
-- order by INGESTION_TIMESTAMP
;