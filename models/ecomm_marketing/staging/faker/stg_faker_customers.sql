{{
    config(
        materialized = 'table'
        , tags = ['faker_customers']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'faker_customers') }}
),

final as (
    select * from staging
)

select * from final