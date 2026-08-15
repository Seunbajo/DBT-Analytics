{{
    config(
        materialized = 'table'
        , tags = ['instruments']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'aptimus_instruments') }}
),

final as (
    select * from staging
)

select * from final