{{
    config(
        materialized = 'table'
        , tags = ['customers']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'electronic_customers') }}
),

final as (
    select * from staging
)

select * from final