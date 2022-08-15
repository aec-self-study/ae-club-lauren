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

    web_sessions as (
        select *
        from {{ ref('web_sessions') }}
    ),


    order_details as (
       
        select 
            orders.*,
            min(created_on) over (partition by orders.customer_id) as customer_first_order_on,
            order_items.product_id,
            products.name as product_name,
            products.category as product_category,
            products.product_available_on,
            product_prices.* except(id, product_id, price_started_at, price_ended_at),
            web_sessions.session_id,
            web_sessions.session_start_at,
            web_sessions.session_end_at,
            round(
                timestamp_diff(
                web_sessions.session_end_at,
                web_sessions.session_start_at,
                second
                ) / 60
                ,2
            ) as session_duration,
            round(
                timestamp_diff(
                web_sessions.event_at,
                web_sessions.session_start_at,
                second
                ) / 60,
                2
            ) as time_to_purchase,
            web_sessions.device_type
        from orders
        left join order_items on 
            orders.id = order_items.order_id
        left join products on 
            order_items.product_id = products.id
        left join product_prices on 
            order_items.product_id = product_prices.product_id and
            orders.created_at >= product_prices.price_started_at and 
            orders.created_at < product_prices.price_ended_at

        left join web_sessions on 
            orders.customer_id = web_sessions.customer_id and 
            orders.created_at = web_sessions.event_at

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
        customer_first_order_on,
        state,
        address,
        zip,
        product_name,
        product_category,
        price_usd,
        session_id,
        session_start_at,
        session_end_at,
        session_duration,
        time_to_purchase,
        device_type
    from order_details