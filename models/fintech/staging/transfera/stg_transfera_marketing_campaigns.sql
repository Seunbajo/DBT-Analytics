{{
    config(
        materialized = 'table'
        , tags = ['marketing_campaigns']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'transfera_marketing_campaigns') }}
),

final as (
    select * from staging
)

select * from final