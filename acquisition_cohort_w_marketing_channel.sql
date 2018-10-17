WITH cte AS (
    -- find previous event to a given transaction and order them
    select row_number() over (partition by a.event_id order by b.derived_tstamp desc) as row_num
    , b.event_id
    , a.tr_orderid as o_id
    from atomic.events AS a
    inner join atomic.events as b on a.domain_userid = b.domain_userid and a.app_id = b.app_id
    where a.event = 'transaction'
    and b.event = 'page_view'
    and b.derived_tstamp <= a.derived_tstamp
    and (b.refr_medium <> 'internal' OR b.refr_medium IS NULL)
    and b.app_id = '1230010009-01'
),
cte2 AS (
    -- join pageview event to the matching marketing channel
    select row_number() over (partition by event_id order by channel_group_id desc) as row_num
        , event_id
        , channel
    from final.event_channel
    where app_id = '1230010009-01'
),
ctex AS (
    -- dedup channel, max channel grouping
    select c2.event_id, c2.channel, c1.o_id
    from cte2 as c2
    inner join cte as c1 on c2.event_id = c1.event_id
    where c2.row_num = 1 
    and c1.row_num = 1
),
cte3 AS (
    -- get all shopify orders and order them in ascending order
    select row_number() over (partition by m.customer__id order by m.created_at asc) as row_num
        , m.customer__id
        , m.order_number
        , m.created_at
        , cx.channel
        , m._mt_account_id
    from shopify.orders as m
    inner join ctex as cx on m.order_number = cx.o_id
    where m._mt_account_id = 2
),
cte4 as (
    -- get each customer's very first order that was from the 'paid search' channel
    select customer__id
        , order_number
        , created_at
        , _mt_account_id
    from cte3
    where channel ILIKE '%paid%search%'
    and row_num = 1
),
cte5 as (
    -- get first order date by customers who's very first order was from the 'paid search' channel
    select so._mt_account_id
        , so.order_number
        , so.customer__id
        , min(so.created_at) as  first_order_date
    from shopify.orders as so
    inner join cte4 as c4 on so.customer__id = c4.customer__id and so._mt_account_id = c4._mt_account_id
    group by 1,2,3
)
    -- get first order date and all other orders by customers who's very first order was from the 'paid search' channel
    select m._mt_account_id
    , m.customer__id
    , m.created_at
    , c5.first_order_date
    , m.total_price
    from shopify.orders as m
    inner join cte5 as c5 on m.customer__id = c5.customer__id and m._mt_account_id = c5._mt_account_id
    group by 1,2,3,4,5
    order by 1
