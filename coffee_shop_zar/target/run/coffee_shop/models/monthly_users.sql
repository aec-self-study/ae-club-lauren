

  create or replace view `aec-students`.`dbt_lauren`.`monthly_users`
  OPTIONS()
  as select
    date_trunc(first_order_at, month) as signup_month,
    count(*) as new_customers
from `aec-students`.`dbt_lauren`.`customers`
group by 
    signup_month;
