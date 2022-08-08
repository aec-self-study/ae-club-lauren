with 
    order_details as (
        select *
        from  {{ ref('order_details') }}
    ),

    weekly_aggregate as (

        select 
          week_of,
          product_category,
          sum(price_usd) as revenue_usd
        from order_details
        group by 
          week_of,
          product_category
    )

    select *
    from weekly_aggregate