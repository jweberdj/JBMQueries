select _mt_account_id, customer__id, rfm_recency*100 + rfm_frequency*10 + rfm_monetary as rfm_combined
from (
    select _mt_account_id,
            customer__id,
            ntile(5) over (order by last_order_date) as rfm_recency,
            ntile(5) over (order by count_order) as rfm_frequency,
            ntile(5) over (order by avg_price) as rfm_monetary
    from (
        select _mt_account_id,
                customer__id,
                max(created_at) as last_order_date,
                count(*) as count_order,
                avg(total_price) as avg_price
        from shopify.orders
        group by 1,2
    )
)
