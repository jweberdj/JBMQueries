WITH cte AS (
    -- find previous event to a given transaction and order them
    select row_number() over (partition by a.domain_userid order by b.derived_tstamp desc) as row_num, b.event_id, a.tr_orderid as o_id
    from atomic.events AS a
    inner join atomic.events as b on a.domain_userid = b.domain_userid
    where a.event = 'transaction'
    and b.event = 'page_view'
    and b.derived_tstamp <= a.derived_tstamp
    and (b.refr_medium <> 'internal' OR b.refr_medium IS NULL)
    and b.app_id = '1230010009-01'
),
cte2 AS (
    -- join pageview event to the matching marketing channel
    select c.event_id, channel, o_id
    from cte as c
    join final.event_channel as f on f.event_id = c.event_id
    where row_num = 1
    and f.app_id = '1230010009-01'
),
cte3 AS (
    select row_number() over (partition by customer__id order by created_at asc) as row_num, customer__id, order_number, created_at
    from shopify.orders
    where _mt_account_id = 2
)
select DATE_TRUNC('month',created_at) as month_, SUM(total_price) as total_revenue, sum(total_revenue) OVER (PARTITION by 1 order by month_ rows unbounded preceding) as cumulative_revenue, COUNT(DISTINCT customer__id) customer_count, SUM(total_price_usd) / COUNT(customer__id) AS revenue_per_customer
from shopify.orders
where created_at between '2017-09-01' and '2019-01-01'
and customer__id in (
    select c3.customer__id
    from cte3 as c3
    inner join cte2 as c2 on c3.order_number = c2.o_id
    where row_num = 1
    and c2.channel ILIKE '%organic%search%'
    and c3.created_at between '2017-10-01' and '2018-01-01'
)
and _mt_account_id = 2
group by 1
order by 1 asc