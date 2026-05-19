{{
    config(
        materialized = 'table'
        , tags = ['support_tickets']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'transfera_support_tickets') }}
),

final as (
    select * from staging
)

select * from final