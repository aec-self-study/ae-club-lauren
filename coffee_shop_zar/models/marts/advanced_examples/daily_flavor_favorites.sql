with dates as (

    -- Get range of min and max date from our data 
    -- Generate a list of dates inclusive in the range
    select date_day
    from unnest(generate_date_array(
      (select min(date(created_at)) from {{ ref('daily_icecream_flavors') }}),
      (select max(date(created_at)) from {{ ref('daily_icecream_flavors') }}),
      interval 1 day
    )) as date_day
  ),

  daily_flavors as (
    select * 
    from {{ ref('daily_icecream_flavors') }}

),

  daily_counts as (

  select 
    dates.date_day,
    daily_flavors.favorite_ice_cream_flavor,
    count(distinct daily_flavors.github_username) as favor_count
  from dates
  left join daily_flavors on
    date_day between date(dbt_valid_from) and ifnull(date(dbt_valid_to), date("2099-01-01"))
  group by 1, 2

  ), 

  daily_ranks as (
    select *,
        row_number() over (partition by date_day order by favor_count desc) as favor_rank
    from daily_counts
  )

  select *
  from daily_ranks 
  where favor_rank = 1
  order by 1

