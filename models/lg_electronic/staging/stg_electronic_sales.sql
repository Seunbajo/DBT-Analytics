{{
    config(
        materialized = 'table'
        , tags = ['sales']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'electronic_sales') }}
),

final as (
    select * from staging
)

select * from final