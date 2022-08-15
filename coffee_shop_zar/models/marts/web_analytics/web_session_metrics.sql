with 
  session_detail as (

    select 
      session_id,
      session_end_at, session_start_at, 
      stitched_visitor_id,
      customer_id,
      device_type,
      page,
      round((timestamp_diff(session_end_at, session_start_at, second) / 60),2) as session_duration,
      case when page = "order-confirmation" then true else false end as was_purchase_made,
      date_trunc(date(session_start_at), month) as session_month,
      date(session_start_at) as session_date,
      extract(dayofweek from date(session_start_at)) as day_of_week,
      extract(hour from (session_start_at)) as hour_of_day
    from {{ ref('web_sessions') }}
  
  ),

  session_metrics as (

  select 
    session_id,
    stitched_visitor_id,
    customer_id,
    session_duration,
    session_month,
    session_date,
    day_of_week,
    hour_of_day,
    count(distinct device_type) as number_of_devices,
    max(was_purchase_made) as was_purchase_made,
    count(page) as pages_visited
  from session_detail
  group by 
    session_id,
    stitched_visitor_id,
    customer_id,
    session_duration,
    session_month,
    session_date,
    day_of_week,
    hour_of_day

  )

  select *
  from session_metrics