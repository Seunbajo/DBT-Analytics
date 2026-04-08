{{
    config(
        materialized = 'table'
        , tags = ['hot_bot_data']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'hot_bot_data') }}
),

final as (
    select * from staging
)

select * from final