{{
    config(
        materialized = 'table'
        , tags = ['prices']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'aptimus_prices') }}
),

final as (
    select * from staging
)

select * from final