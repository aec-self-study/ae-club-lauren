with 
    order_details as (
        select *,
          first_value(week_of) over (partition by customer_id order by created_at rows between unbounded preceding and unbounded following)
          as first_order_week_of
        from  {{ ref('order_details') }}
    ),

    weekly_aggregate as (
        select 
          week_of,
          sum(case when week_of = first_order_week_of then price_usd else 0 end) as new_customer_revenue_usd,
          sum(case when week_of > first_order_week_of then price_usd else 0 end) as existing_customer_revenue_usd
        from order_details
        group by 
          week_of

    )

    select *
    from weekly_aggregate

       