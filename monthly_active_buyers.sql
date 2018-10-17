select COUNT(distinct customer__id) active_buyer_count
from shopify.orders
where _mt_account_id = 1
and customer__id in (
    select customer__id
    from shopify.orders
    where _mt_account_id = 1
    and created_at between '2017-07-31' and '2018-07-31'
    group by 1 having count(*) >= 1
)