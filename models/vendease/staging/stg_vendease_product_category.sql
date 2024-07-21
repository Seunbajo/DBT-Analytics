{{
    config(
        materialized = 'table'
        , tags = ['vendease_product_category']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'vendease_product_category') }}
),

final as (
    select * from staging
)

select * from final