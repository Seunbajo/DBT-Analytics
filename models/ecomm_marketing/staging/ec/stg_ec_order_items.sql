{{
    config(
        materialized = 'table'
        , tags = ['orders']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'ec_order_items') }}
),

final as (
    select * from staging
)

select * from final