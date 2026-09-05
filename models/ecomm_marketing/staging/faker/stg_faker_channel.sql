{{
    config(
        materialized = 'table'
        , tags = ['faker_channel']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'faker_channel') }}
),

final as (
    select * from staging
)

select * from final