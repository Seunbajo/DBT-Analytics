{{
    config(
        materialized = 'table'
        , tags = ['vendease_order_items']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'vendease_order_items') }}
),

final as (
    select * from staging
)

select * from final