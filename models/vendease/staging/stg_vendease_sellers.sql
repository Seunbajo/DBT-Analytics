{{
    config(
        materialized = 'table'
        , tags = ['vendease_sellers']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'vendease_sellers') }}
),

final as (
    select * from staging
)

select * from final