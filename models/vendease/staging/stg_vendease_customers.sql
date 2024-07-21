{{
    config(
        materialized = 'table'
        , tags = ['vendease_customers']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'vendease_customers') }}
),

final as (
    select * from staging
)

select * from final