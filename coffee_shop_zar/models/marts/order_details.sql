with 
    orders as (
        select *
        from  {{ ref('stg_coffee_shop__orders') }}
    ),


    order_items as (
        select *
        from  {{ ref('stg_coffee_shop__order_items') }}
    ),


    products as (
        select *
        from  {{ ref('stg_coffee_shop__products') }}
    ),
    
    product_prices as (
        select *
        from  {{ ref('stg_coffee_shop__product_prices') }}
    ),


    order_details as (
       
        select 
            orders.*,
            order_items.product_id,
            products.name as product_name,
            products.category as product_category,
            products.product_available_on,
            product_prices.* except(id, product_id, price_started_at, price_ended_at)
        from orders
        left join order_items on 
            orders.id = order_items.order_id
        left join products on 
            order_items.product_id = products.id
        left join product_prices on 
            order_items.product_id = product_prices.product_id and
            orders.created_at >= product_prices.price_started_at and 
            orders.created_at < product_prices.price_ended_at

    )

    select 
        id,
        created_at,
        created_on,
        year_of, 
        month_of,
        week_of,
        day_of_week_name,
        customer_id,
        state,
        address,
        zip,
        product_name,
        product_category,
        product_available_on,
        price_usd
    from order_details