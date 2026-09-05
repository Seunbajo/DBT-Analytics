{{
    config(
        materialized = 'table'
        , tags = ['faker_campaigns']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'faker_campaigns') }}
),

final as (
    select * from staging
)

select * from final