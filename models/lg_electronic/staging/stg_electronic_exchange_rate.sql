{{
    config(
        materialized = 'table'
        , tags = ['exchange_rate']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'electronic_exchange_rate') }}
),

final as (
    select * from staging
)

select * from final