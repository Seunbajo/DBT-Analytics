{{
    config(
        materialized = 'table'
        , tags = ['nfex_dollar_tbale']
    )
}}

with staging as (
    select * from {{ source('dbt_projects_services', 'nfex_dollar_rate') }}
),

final as (
    select * from staging
)

select * from final