-- average order value of orders 2,3,4...x for repeat customers and counts of orders for each ordinal
with cte as(select ROW_NUMBER () OVER (PARTITION BY domain_userid ORDER BY derived_tstamp) AS order_ordinal
,  tr_total
from atomic.events
where app_id = '1230010065-01'
 AND event = 'transaction'
 AND derived_tstamp BETWEEN '2017-10-04' AND '2018-06-18'
)
select order_ordinal,COUNT(order_ordinal), avg(tr_total)
from cte
group by 1 having COUNT(order_ordinal) > 1
order by 1
