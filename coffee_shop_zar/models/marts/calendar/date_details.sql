with date_spine AS (
    
    select date_day
    from (
      select generate_date_array("2018-01-01", date_add(current_date, interval 20 year), interval 1 day) as dates
    ), 
    unnest(dates) as date_day

),


calculated as (

    select
      date_day,
      date_day as date_actual,
      format_date('%A', date_day) as day_name,
      extract(month from date_day) as month_actual,
      extract(year from date_day) as year_actual,
      extract(quarter from date_day) as quarter_actual,
      extract(week from date_day) as week_actual,
      date_trunc(date_day, week) as week_start_on,
      date_add(date_trunc(date_day, week), interval 6 day) as week_end_on,
      date_trunc(date_day, month) as first_day_of_month,
      last_day(date_day) as last_day_of_month,
      (extract(year from date_day) || '-Q' || extract(quarter from date_day)) as quarter_name
    from date_spine
)

select *
from calculated
