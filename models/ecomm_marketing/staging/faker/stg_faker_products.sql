{{
    config(
        materialized = 'table'
        , tags = ['faker_products']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'faker_products') }}
),

final as (
    select * from staging
)

select * from final