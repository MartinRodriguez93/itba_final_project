--customer support

--Average Delivery Time
;
SELECT 
    AVG(DATEDIFF('day', ORDER_PURCHASE_AT, ORDER_DELIVERED_CUSTOMER_AT)) AS avg_delivery_time_days
from intermediate.marketing.orders_join_customers
WHERE 
    ORDER_DELIVERED_CUSTOMER_AT IS NOT NULL
      AND ORDER_PURCHASE_AT IS NOT NULL
;

-- On-time Delivery Rate
;
SELECT 
    (COUNT(CASE 
            WHEN ORDER_DELIVERED_CUSTOMER_AT <= ORDER_ESTIMATED_DELIVERY_AT 
            THEN 1 
          END) * 100.0 / COUNT(*)) AS on_time_delivery_rate
from intermediate.marketing.orders_join_customers
WHERE 
    ORDER_DELIVERED_CUSTOMER_AT IS NOT NULL
      AND ORDER_ESTIMATED_DELIVERY_AT IS NOT NULL
;

--Repeat Customer Rate
-- This query calculates the percentage of orders that come from repeat customers (customers with more than one order).
;
WITH customer_order_counts AS (
    SELECT 
        customer_unique_id, 
        COUNT(ORDER_ID) AS order_count
    from intermediate.marketing.orders_join_customers
    GROUP BY 1
)
SELECT 
    (COUNT(CASE 
            WHEN order_count > 1 
            THEN 1 
          END) * 100.0 / COUNT(*)) AS repeat_customer_rate
FROM customer_order_counts
;

--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

;
select
    *
-- from intermediate.finance.orders_join_payments
-- from intermediate.marketing.order_items_join_products
-- from intermediate.marketing.order_items_join_sellers
-- from intermediate.customer_support.order_reviews_join_orders
-- limit 10
;

--marketing
--finance