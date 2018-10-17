-- average time between initial pageview and next transaction in a given time period
WITH users AS (
    SELECT domain_userid,
        event_id,
        derived_tstamp,
        row_number() over (partition by domain_userid order by derived_tstamp) as n
    FROM atomic.events
    WHERE page_url LIKE '%woodwind%promotion%'
    AND   derived_tstamp BETWEEN '2018-03-22' AND '2018-06-05'
),
pv AS (
    select * from users where n=1 
),
tx AS(
    SELECT DISTINCT
            tr.event_id,
            tr.derived_tstamp as tx_time,
            tr.domain_userid,
            pv.derived_tstamp as page_view_time
    FROM atomic.events tr
    JOIN pv ON pv.domain_userid = tr.domain_userid
    WHERE tr.derived_tstamp BETWEEN '2018-03-22' AND '2018-06-05'
    AND   tr.event = 'transaction'
) 
select AVG(datediff(hours, page_view_time, tx_time))/24
from tx
where tx_time > page_view_time;
