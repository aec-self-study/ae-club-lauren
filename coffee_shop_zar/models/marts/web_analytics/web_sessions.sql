{{ config(
    materialized='table'
    ) 
 }}


with 
  web_tracking as (
    select *,
      first_value(customer_id ignore nulls) over (partition by visitor_id order by timestamp rows between unbounded preceding and unbounded following) as _customer_id
    from {{source('web_tracking', 'pageviews') }}
  ),

  id_combos as (
    select  
      customer_id,
      to_hex(md5(string_agg(visitor_id order by visitor_id))) as stitched_visitor_id
    from web_tracking
    where 
      customer_id is not null
    group by 
      customer_id

  ),

  stitched as (

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

  ),

  add_previous_next_event as (

    select *,
      lag(event_at) over (partition by stitched_visitor_id order by event_at) as previous_event_at,
      lead(event_at) over (partition by stitched_visitor_id order by event_at) as next_event_at
    from stitched

  ),

  calculate_start_end as (

      select *,
        case 
          when timestamp_diff(event_at, previous_event_at, minute) > 30 or previous_event_at is null then true else false 
        end as is_start,
        case 
          when timestamp_diff(next_event_at, event_at, minute) > 30 or next_event_at is null then true else false 
        end as is_end,
      from add_previous_next_event

  ),

  add_session_id as (

      select *,
        to_hex(md5(
          concat(stitched_visitor_id,
          sum(case when is_start then 1 else 0 end) over (partition by stitched_visitor_id order by event_at rows between unbounded preceding and current row)
          )
        )) as session_id
      from calculate_start_end

  ),



  add_session_start_end as (

  select 
    id,
    session_id,
    min(event_at) over (partition by session_id) as session_start_at,
    max(event_at) over (partition by session_id) as session_end_at,
    event_at,
    customer_id,
    stitched_visitor_id,
    device_type,
    page
  from add_session_id

  )

  select *
  from add_session_start_end