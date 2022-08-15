with 
  web_tracking as (
    select *,
      first_value(customer_id ignore nulls) over (partition by visitor_id order by timestamp rows between unbounded preceding and unbounded following) as _customer_id
    from `analytics-engineers-club.web_tracking.pageviews`
  ),

  id_combos as (
    select  
      customer_id,
      string_agg(visitor_id) as stitched_visitor_id
    from web_tracking
    where 
      customer_id is not null
    group by 
      customer_id

  )

 select 
  web_tracking.id,
  web_tracking.device_type,
  web_tracking.timestamp as event_at,
  web_tracking.page,
  web_tracking._customer_id as customer_id,
  ifnull(id_combos.stitched_visitor_id, web_tracking.visitor_id) as stitched_visitor_id
 from web_tracking
 left join id_combos on 
   web_tracking._customer_id = id_combos.customer_id

order by stitched_visitor_id, timestamp
