{{
    config(
        materialized = 'table'
        , tags = ['countries']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'transfera_countries') }}
),

final as (
    select * from staging
)

select * from final