{{
    config(
        materialized = 'table'
        , tags = ['vendease_orders']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'vendease_orders') }}
),

final as (
    select * from staging
)

select * from final