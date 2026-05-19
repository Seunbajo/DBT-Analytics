{{
    config(
        materialized = 'table'
        , tags = ['fx_rates']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'transfera_fx_rates') }}
),

final as (
    select * from staging
)

select * from final