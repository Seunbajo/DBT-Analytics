{{
    config(
        materialized = 'table'
        , tags = ['stores']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'electronic_stores') }}
),

final as (
    select * from staging
)

select * from final