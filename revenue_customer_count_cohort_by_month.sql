-- acquisition_month | retention_month | customer_count | revenue_total
with a as (
    select _mt_account_id,customer__id, min(created_at) as  first_order_date
    from shopify.orders
    where _mt_account_id = 1
    group by 1,2
)
select m._mt_account_id,m.customer__id, m.created_at, a.first_order_date, m.total_price
from shopify.orders as m
inner join a on m.customer__id = a.customer__id and m._mt_account_id = a._mt_account_id
group by 1,2,3,4
order by 1


