SELECT COUNT(DISTINCT customer__id) AS active_customer_count
FROM shopify.orders
WHERE customer__id IN (
    SELECT customer__id
    FROM shopify.orders
    WHERE _mt_account_id = 2
    AND created_at BETWEEN DATEADD(MONTH, -18, '2017-07-31') AND '2017-07-31'
    GROUP BY 1 HAVING COUNT(*) >= 1
)
AND _mt_account_id = 2