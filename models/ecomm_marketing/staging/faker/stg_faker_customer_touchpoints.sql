{{
    config(
        materialized = 'table'
        , tags = ['faker_customer_touchpoints']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'faker_customer_touchpoints') }}
),

final as (
    select * from staging
)

select * from final