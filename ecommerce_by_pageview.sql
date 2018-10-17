select EXTRACT(week from derived_tstamp) week,COUNT(DISTINCT domain_sessionid) sessions,COUNT(DISTINCT network_userid) users,COUNT(tr_total) transactions,SUM(tr_total) revenue, SUM(tr_total) / COUNT(tr_total) aov, (COUNT(tr_total) / COUNT(domain_sessionid))*100::FLOAT cvr
from atomic.events
 where network_userid in
(
  select distinct network_userid
  from atomic.events
  where page_url like '%homage%go%tos%'
  and app_id = '1230010009-01'
  and derived_tstamp between '2018-03-22' and '2018-05-22'
)
and derived_tstamp between '2018-03-22' and '2018-05-22'
and app_id = '1230010009-01'
group by 1
order by 1
