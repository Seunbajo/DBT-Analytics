{{
    config(
        materialized = 'table'
        , tags = ['faker_order_items']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'faker_order_items') }}
),

final as (
    select * from staging
)

select * from final