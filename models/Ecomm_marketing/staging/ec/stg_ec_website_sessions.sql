{{
    config(
        materialized = 'table'
        , tags = ['website_sessions']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'ec_website_sessions') }}
),

final as (
    select * from staging
)

select * from final