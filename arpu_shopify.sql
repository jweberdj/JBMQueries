select DATE_TRUNC('month',created_at) _month, sum(total_price_usd) as row_sum, sum(row_sum) OVER (PARTITION by 1 order by _month rows unbounded preceding) as row_num, COUNT(customer__id) customer_count, SUM(total_price_usd) / COUNT(customer__id) AS revenue_per_customer
from shopify.orders
where customer__id in (
        -- all customer id's from Q1 2017 for new customers
    select customer__id
    from shopify.orders
    where _mt_account_id = 2
    group by 1 having min(created_at) between '2017-01-01' and '2017-03-31'
)
and _mt_account_id = 2
and created_at between '2017-01-01' and '2019-01-01'
group by 1
order by 1