{{
    config(
        materialized = 'table'
        , tags = ['vendease_products']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'vendease_products') }}
),

final as (
    select * from staging
)

select * from final