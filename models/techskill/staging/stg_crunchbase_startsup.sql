{{
    config(
        materialized = 'table'
        , tags = ['crunchbase_startsup']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'crunchbase_startsup') }}
),

final as (
    select * from staging
)

select * from final