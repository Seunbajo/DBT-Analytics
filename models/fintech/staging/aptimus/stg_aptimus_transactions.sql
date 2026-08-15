{{
    config(
        materialized = 'table'
        , tags = ['transactions']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'aptimus_transactions') }}
),

final as (
    select * from staging
)

select * from final