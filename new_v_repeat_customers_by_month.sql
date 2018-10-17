-- New vs Repeat Customers - Share of Total Sales by Month
SELECT
    month_, customer_type, COUNT(1)
FROM
    (
        SELECT 
            DATE_TRUNC('month',created_at) as month_,
            customer__id,
            CASE ROW_NUMBER () OVER (PARTITION BY customer__id ORDER BY created_at)
                WHEN 1
                    THEN 'new_customer'
                ELSE 'repeat_customer'
            END as customer_type
        FROM shopify.orders
        WHERE _mt_account_id = 1
    ) as temp
group by 1,2
order by 1