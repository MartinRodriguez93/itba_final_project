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

--finance

--Daily Payment Count by Type
SELECT
    PAYMENT_DATE,
    PAYMENT_TYPE,
    COUNT(*) AS payment_count
FROM intermediate.finance.orders_join_payments
GROUP BY 1,2
ORDER BY 1;

--Daily Total installment payments:
SELECT
    PAYMENT_DATE,
    --only payment_type = 'credit_card' has >1 installments
    SUM(CASE WHEN PAYMENT_INSTALLMENTS > 1 THEN 1 ELSE 0 END) AS total_installments,
    SUM(CASE WHEN PAYMENT_INSTALLMENTS > 1 THEN PAYMENT_VALUE ELSE 0 END) AS total_installment_value
FROM intermediate.finance.orders_join_payments
GROUP BY 1
ORDER BY 1;

-- Daily Payment Tracking
SELECT 
    PAYMENT_DATE, 
    SUM(PAYMENT_VALUE) AS daily_payment_total
FROM intermediate.finance.orders_join_payments
GROUP BY 1
ORDER BY 1;

-- Top customers by payment value
WITH total_payments AS (
    SELECT SUM(PAYMENT_VALUE) AS total_payment_value
    FROM intermediate.finance.orders_join_payments
)
SELECT 
    CUSTOMER_UNIQUE_ID, 
    SUM(PAYMENT_VALUE) AS total_payment_value,
    ROUND(total_payment_value / (SELECT total_payment_value FROM total_payments) * 100, 2) AS percentage_of_total
FROM intermediate.finance.orders_join_payments
GROUP BY 1
ORDER BY 3 DESC;

-- Payment contribution by region
WITH total_payments AS (
    SELECT SUM(PAYMENT_VALUE) AS total_payment_value
    FROM intermediate.finance.orders_join_payments
)
SELECT 
    CUSTOMER_STATE,
    COUNT(DISTINCT CUSTOMER_UNIQUE_ID) AS total_unique_customers,
    SUM(PAYMENT_VALUE) AS total_payment_value,
    ROUND(total_payment_value / (SELECT total_payment_value FROM total_payments) * 100, 2) AS percentage_of_total
FROM intermediate.finance.orders_join_payments
GROUP BY 1
ORDER BY 4 DESC;

-- Payment contribution by city
WITH total_payments AS (
    SELECT SUM(PAYMENT_VALUE) AS total_payment_value
    FROM intermediate.finance.orders_join_payments
)
SELECT 
    CUSTOMER_CITY,
    COUNT(DISTINCT CUSTOMER_UNIQUE_ID) AS total_unique_customers,
    SUM(PAYMENT_VALUE) AS total_payment_value,
    ROUND(total_payment_value / (SELECT total_payment_value FROM total_payments) * 100, 2) AS percentage_of_total
FROM intermediate.finance.orders_join_payments
GROUP BY 1
ORDER BY 4 DESC;

--payment contribution by CUSTOMER_CITY for SP
WITH total_payments AS (
    SELECT SUM(PAYMENT_VALUE) AS total_payment_value
    FROM intermediate.finance.orders_join_payments
where
    CUSTOMER_STATE = 'SP'   
)
SELECT 
    CUSTOMER_CITY, 
    SUM(PAYMENT_VALUE) AS total_payment_value,
    ROUND(total_payment_value / (SELECT total_payment_value FROM total_payments) * 100, 2) AS percentage_of_total
FROM intermediate.finance.orders_join_payments
where
    CUSTOMER_STATE = 'SP'
group by 1
ORDER BY 2 DESC;

--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

--marketing

;
select
    *
-- from intermediate.marketing.order_items_join_products
-- from intermediate.marketing.order_items_join_sellers
-- from intermediate.customer_support.order_reviews_join_orders
-- limit 10
;
