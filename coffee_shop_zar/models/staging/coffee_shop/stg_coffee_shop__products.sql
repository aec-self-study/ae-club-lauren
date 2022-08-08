with 
    products as (
        select *
        from  {{ source('coffee_shop', 'products') }}
    ),

    renamed as (
        select 
            id,
            name,
            category,
            created_at as product_available_on
        from products

    )

    select *
    from renamed