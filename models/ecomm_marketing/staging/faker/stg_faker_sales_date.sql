{{
    config(
        materialized = 'table'
        , tags = ['faker_sales_date']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'faker_sales_date') }}
),

final as (
    select * from staging
)

select * from final