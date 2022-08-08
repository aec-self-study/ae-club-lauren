with 

    product_prices as (
        select *
        from  {{ source('coffee_shop', 'product_prices') }}
    ),

    renamed as (
        select 
            id,
            product_id,
            price as price_usd,
            created_at as price_started_at,
            ended_at as price_ended_at
        from product_prices
    )

    select *
    from renamed