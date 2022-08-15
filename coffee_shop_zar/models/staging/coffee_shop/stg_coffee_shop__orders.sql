with 
    orders as (
        select *
        from  {{ source('coffee_shop', 'orders') }}
    ),

    renamed as (

        select 
            id,
            created_at as created_at,
            date(created_at) as created_on,
            extract(year from date(created_at)) as year_of,
            date_trunc(date(created_at), month) as month_of,
            date_trunc(date(created_at), week) as week_of,
            case extract(dayofweek from date(created_at)) 
                when 1 then "Sunday"
                when 2 then "Monday"
                when 3 then "Tuesday"
                when 4 then "Wednesday"
                when 5 then "Thursday"
                when 6 then "Friday"
                when 7 then "Saturday"
            end as day_of_week_name,
            customer_id,
            total as total_amount_usd,
            state,
            address,
            zip
        from orders

    )

    select *
    from renamed