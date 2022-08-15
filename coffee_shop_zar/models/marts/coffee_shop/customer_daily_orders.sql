{{ config(
    materialized='table'
    ) 
 }}
{% set device_types = dbt_utils.get_column_values(table=ref('order_details'), column='device_type') %}



with
  orders as (
    select *
    from {{ ref('order_details') }}
  ),

  daily_orders as (
    select
      created_on,
      customer_id,
      customer_first_order_on,
      state,
      address,
      zip,
      count(id) as purchase_count,
      sum(price_usd) as revenue_amount,
      count(distinct session_id) as session_count,
      avg(session_duration) as average_session_duration,

      {% for device_type in device_types %}
      sum(case when device_type = '{{ device_type }}' then 1 else 0 end) as {{ device_type | lower | replace(" ", "_") }}_count,
      sum(case when device_type = '{{ device_type }}' then price_usd else 0 end) as {{ device_type | lower | replace(" ", "_") }}_amount,
      {% endfor %}
    from orders
    group by 
      created_on,
      customer_id,
      customer_first_order_on,
      state,
      address,
      zip
  
  ),

  date_details as (
    select *
    from `aec-students.dbt_lauren.date_details`
    where 
      date_day >= date("2020-12-27") and
      date_day <= date("2021-10-01")
  ),


  date_spine as (
    select 
      date_details.*,
      orders.customer_id,
      orders.customer_first_order_on
    from date_details
    inner join (select distinct customer_id, customer_first_order_on from daily_orders) as orders on 
      date_details.date_actual >= orders.customer_first_order_on
  ),

  final as (
    
    select 
      date_spine.date_actual,
      date_spine.day_name,
      date_spine.month_actual,
      date_spine.year_actual,
      date_spine.quarter_actual,
      date_spine.week_actual,
      date_spine.week_start_on,
      date_spine.week_end_on,
      date_spine.first_day_of_month,
      date_spine.last_day_of_month,
      date_spine.quarter_name,
      date_spine.customer_id,
      date_spine.customer_first_order_on,
      ifnull(daily_orders.purchase_count,0) as purchase_count,
      ifnull(daily_orders.revenue_amount,0) as revenue_amount,
      ifnull(daily_orders.session_count,0) as session_count,
      ifnull(daily_orders.average_session_duration,0) as average_session_duration,

      {% for device_type in device_types %}
      ifnull( {{ device_type | lower | replace(" ", "_") }}_count, 0) as {{ device_type | lower | replace(" ", "_") }}_count,
      ifnull( {{ device_type | lower | replace(" ", "_") }}_amount, 0) as {{ device_type | lower | replace(" ", "_") }}_amount,
      {% endfor %}
    

    from date_spine
    left join daily_orders on 
      date_spine.date_actual = daily_orders.created_on and 
      date_spine.customer_id = daily_orders.customer_id

  )

  select *
  from final