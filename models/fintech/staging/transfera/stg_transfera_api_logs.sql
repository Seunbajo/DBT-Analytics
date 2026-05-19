{{
    config(
        materialized = 'table'
        , tags = ['api_logs']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'transfera_api_logs') }}
),

final as (
    select * from staging
)

select * from final