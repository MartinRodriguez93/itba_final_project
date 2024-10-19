--sales/logistics

--Average Delivery Time
;
SELECT 
    AVG(DATEDIFF('day', ORDER_PURCHASE_AT, ORDER_DELIVERED_CUSTOMER_AT)) AS avg_delivery_time_days
from intermediate.marketing.orders_join_customers_reviews
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
from intermediate.marketing.orders_join_customers_reviews
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
    from intermediate.marketing.orders_join_customers_reviews
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

--Total sales and quantity of orders sold for each category
;
WITH category_sales AS (
  SELECT 
    PRODUCT_CATEGORY_NAME,
    SUM(PRICE) AS TOTAL_SALES,
    COUNT(ORDER_ITEM_ID) AS TOTAL_QUANTITY
  FROM intermediate.marketing.order_items_join_sellers_products
  GROUP BY 1
),
total_sales AS (
  SELECT SUM(TOTAL_SALES) AS GRAND_TOTAL
  FROM category_sales
)
SELECT 
  cs.PRODUCT_CATEGORY_NAME,
  cs.TOTAL_SALES,
  cs.TOTAL_QUANTITY,
  ROUND((cs.TOTAL_SALES / ts.GRAND_TOTAL) * 100, 2) AS SALES_PERCENTAGE
FROM category_sales cs, total_sales ts
order by 2 desc
;

--Total sales for each seller
;
WITH seller_sales AS (
  SELECT 
    SELLER_ID,
    SUM(PRICE) AS TOTAL_SALES
  FROM intermediate.marketing.order_items_join_sellers_products
  GROUP BY 1
),
total_seller_sales AS (
  SELECT SUM(TOTAL_SALES) AS GRAND_TOTAL
  FROM seller_sales
)
SELECT 
  ss.SELLER_ID,
  ss.TOTAL_SALES,
  ROUND((ss.TOTAL_SALES / tss.GRAND_TOTAL) * 100, 2) AS SALES_PERCENTAGE
FROM seller_sales ss, total_seller_sales tss
order by 2 desc
;

--XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

-- customer support

-- Average REVIEW_SCORE by CUSTOMER_ID
;
SELECT 
    CUSTOMER_ID, 
    AVG(REVIEW_SCORE) AS AVERAGE_REVIEW_SCORE
from intermediate.marketing.orders_join_customers_reviews
WHERE REVIEW_SCORE IS NOT NULL
GROUP BY 1;

--Percentage of Orders with Reviews
;
WITH total_orders AS (
  SELECT COUNT(ORDER_ID) AS TOTAL_ORDERS
  FROM intermediate.marketing.orders_join_customers_reviews
),
orders_with_reviews AS (
  SELECT COUNT(ORDER_ID) AS ORDERS_WITH_REVIEWS
  FROM intermediate.marketing.orders_join_customers_reviews
  WHERE REVIEW_ID IS NOT NULL
)
SELECT 
    (owr.ORDERS_WITH_REVIEWS / tor.TOTAL_ORDERS) * 100 AS PERCENTAGE_ORDERS_WITH_REVIEWS
FROM orders_with_reviews owr, total_orders tor
;

--. Response Time Analysis (Difference between REVIEW_CREATION_AT and REVIEW_ANSWER_AT)
;
SELECT 
    CUSTOMER_ID, 
    REVIEW_ID, 
    DATEDIFF('day', REVIEW_CREATION_AT, REVIEW_ANSWER_AT) AS RESPONSE_TIME_DAYS
FROM intermediate.marketing.orders_join_customers_reviews
WHERE 
    REVIEW_CREATION_AT IS NOT NULL
    AND REVIEW_ANSWER_AT IS NOT NULL
ORDER BY 3 DESC
;