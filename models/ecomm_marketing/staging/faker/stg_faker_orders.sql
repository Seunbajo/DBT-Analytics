{{
    config(
        materialized = 'table'
        , tags = ['faker_orders']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'faker_orders') }}
),

final as (
    select * from staging
)

select * from final