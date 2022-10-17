{% snapshot  daily_icecream_flavors %}
 
{{ config(
      target_schema='dbt_lauren_snapshots',
      unique_key='github_username',
      strategy='check',
      check_cols=['favorite_ice_cream_flavor']
 ) }}
 
select
    *
 from {{source("advanced_dbt_examples","favorite_ice_cream_flavors")}}
 
{% endsnapshot %}