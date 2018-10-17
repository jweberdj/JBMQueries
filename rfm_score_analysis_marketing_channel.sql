WITH cte AS (
    -- find previous event to a given transaction and order them
    select row_number() over (partition by a.domain_userid order by b.derived_tstamp desc) as row_num, a.domain_userid, b.event_id, a.tr_orderid as o_id
    from atomic.events AS a
    inner join atomic.events as b on a.domain_userid = b.domain_userid
    where a.event = 'transaction'
    and b.event = 'page_view'
    and b.derived_tstamp <= a.derived_tstamp
    and b.refr_medium <> 'internal'
    and b.app_id = '1230010009-01'
),
cte2 AS (
    -- join pageview event to the matching marketing channel
    select c.event_id, channel, o_id, domain_userid
    from cte as c
    join final.event_channel as f on f.event_id = c.event_id
    where row_num = 1
    and f.app_id = '1230010009-01'
),
cte3 AS (
    -- join shopify order data with marketing channel
   SELECT _mt_account_id, customer__id, created_at, total_price
   from shopify.orders
   where customer__id in (
        SELECT s.customer__id
        from shopify.orders as s
        inner join cte2 as c2 on c2.o_id = s.order_number
        WHERE _mt_account_id = 2
        AND c2.channel ILIKE '%paid%search%'
        GROUP BY 1 --HAVING MIN(created_at) BETWEEN '2017-01-01' AND '2017-12-31'
   )
)
select _mt_account_id, customer__id, rfm_recency*100 + rfm_frequency*10 + rfm_monetary as rfm_combined
from (
    select _mt_account_id,
            customer__id,
            ntile(5) over (order by last_order_date) as rfm_recency,
            ntile(5) over (order by count_order) as rfm_frequency,
            ntile(5) over (order by avg_price) as rfm_monetary
    from (
        select so._mt_account_id,
                so.customer__id,
                max(so.created_at) as last_order_date,
                count(so.*) as count_order,
                avg(so.total_price) as avg_price
        from shopify.orders as so
        inner join cte3 as c3 on so.customer__id = c3.customer__id
        group by 1,2
    )
)