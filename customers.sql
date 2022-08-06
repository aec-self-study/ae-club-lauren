

/*
For each customer, we want to know how many orders they have placed, and when they placed their first order.
*/

with 
    customers as (
      select * from `analytics-engineers-club.coffee_shop.customers`
    ),

    orders as (
      select * from `analytics-engineers-club.coffee_shop.orders`
    ),

    aggregate_orders as (
      select customer_id,
            count(distinct id) as orders,
            array_agg(created_at order by created_at asc limit 1)[safe_offset(0)] as first_order_at
      from orders
      group by customer_id
    ),

    final as (

      select customers.*,
            aggregate_orders.* except(customer_id)
      from customers 
      left join aggregate_orders on 
        customers.id = aggregate_orders.customer_id

    )

    select *
    from final
    order by first_order_at