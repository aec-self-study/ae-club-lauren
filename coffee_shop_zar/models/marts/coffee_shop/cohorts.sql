

{{ config(
    materialized='table'
    ) 
 }}

with

  weekly_customer_revenue as (
    select 
      customer_id,
      week_of,
      min(week_of) over (partition by customer_id) as cohort_week,
      min(week_of) over () as reference_start_date,
      max(week_of) over () as reference_end_date,
      sum(price_usd) as revenue
    from  {{ ref('order_details') }}
    group by 
      customer_id,
      week_of
  ),

  date_details as (
    select *
    from  {{ ref('date_details') }}
    where 
      date_actual = week_start_on and 
      date_actual >= {{ var('start_date') }} and 
      date_actual <= {{ var('end_date') }}

  ),


  date_spine as (
    select 
      date_details.week_start_on,
      date_details.week_end_on,
      date_details.quarter_name,
      cohorts.customer_id,
      cohorts.cohort_week
    from date_details
    cross join (select distinct customer_id, cohort_week, reference_start_date, reference_end_date  from weekly_customer_revenue ) as cohorts
    where date_details.week_start_on >= cohorts.cohort_week and
      date_details.week_start_on >= cohorts.reference_start_date and
      date_details.week_start_on <= cohorts.reference_end_date
  ),

  cohort_details as (
    select 
      date_spine.week_start_on,
      date_spine.week_end_on,
      date_spine.quarter_name,
      date_spine.customer_id,
      date_spine.cohort_week,
      ifnull(weekly_customer_revenue.revenue,0) as revenue, 
      row_number() over (partition by date_spine.customer_id order by date_spine.week_start_on) as week,
      sum(ifnull(weekly_customer_revenue.revenue,0)) over (partition by date_spine.customer_id order by date_spine.week_start_on rows between unbounded preceding and current row) as cumulative_revenue
    from date_spine
    left join weekly_customer_revenue on 
      date_spine.customer_id = weekly_customer_revenue.customer_id and 
      date_spine.week_start_on = weekly_customer_revenue.week_of 
  ),


  cohort_revenues as (
    select 
        week,
        cohort_week,
        count(distinct customer_id) as cohort_size,
        sum(revenue) as cohort_total_revenue,
        sum(cumulative_revenue) as cohort_total_cumulative_revenue
    from cohort_details
    group by 1,2
  ),


normalized_cohorts as (
    select
        *,
        cohort_total_revenue / cohort_size as average_revenue,
        cohort_total_cumulative_revenue / cohort_size as average_ltv
    from cohort_revenues
    
)


select *
from normalized_cohorts
order by cohort_week, week



