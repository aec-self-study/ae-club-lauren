{{ config(
    materialized='incremental',
    unique_key='id',
    sort = 'timestamp'
 ) }}
 
with 

{% if is_incremental() %}
incremental_users_source as (
    select distinct 
    github_username
    from {{ source('advanced_dbt_examples', 'form_events') }}
    where timestamp > (select max(form_entry_at) from {{ this }} )
),
{% endif %}


events_source as (
    select *
    from {{ source('advanced_dbt_examples', 'form_events') }} as events
    {% if is_incremental() %}
    where exists (
        select 1 
        from incremental_users_source as users 
        where events.github_username = users.github_username
    )
    {% endif %}
),

 
aggregated as (
    select
        md5(github_username || '-' || timestamp) as id,
        github_username,
        timestamp as form_entry_at,
        row_number() over (partition by github_username order by timestamp) as submission_sequence,
        row_number() over (partition by github_username order by timestamp) = 1 as is_first_submission,
        min(timestamp) over (partition by github_username)  as first_form_entry,
        max(timestamp) over (partition by github_username)  as last_form_entry
    from events_source
 )
 
select * from aggregated