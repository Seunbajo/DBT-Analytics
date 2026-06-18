{{
    config(
        materialized = 'table'
        , tags = ['commission']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'corridor_commission') }}
),

final as (
    select * from staging
)

select * from final