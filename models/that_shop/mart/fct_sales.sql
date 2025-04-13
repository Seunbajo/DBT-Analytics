{{
    config(
        materialized = 'incremental'
        , tags = ['that_shop']
    )
}}

with sales as (
    select * from {{ ref('stg_sales') }}
)

, final as (
    select
        * 
        , case 
            when total_value >= 300 then 'highest_end_product'
            when total_value >= 200 then 'high_end_product'
            when total_value >= 100 then 'middle_end_product'
            when total_value >= 50 then 'low_end_product'
            else 'very_low_end_product'
          end as class_of_product
    from sales
)

select * from final