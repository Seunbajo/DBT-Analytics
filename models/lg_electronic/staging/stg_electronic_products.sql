{{
    config(
        materialized = 'table'
        , tags = ['products']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'electronic_products') }}
),

final as (
    select * from staging
)

select * from final