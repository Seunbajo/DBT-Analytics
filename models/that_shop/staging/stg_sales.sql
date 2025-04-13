{{
    config(
        materialized = 'table'
        , tags = ['that_shop_sales']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'that_shop_sales_data') }}
),

final as (
    select
        *
    from staging
)

select * from final