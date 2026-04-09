{{
    config(
        materialized = 'table'
        , tags = ['orders']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'ec_ad_performance') }}
),

final as (
    select * from staging
)

select * from final