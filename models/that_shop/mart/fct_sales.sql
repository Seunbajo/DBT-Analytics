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
            when total_value between 300 and 201 then 'highest_end_product'
            when total_value between 200 and 101 then 'high_end_product'
            when total_value between 100 and 50 then 'middle_end_product'
            when total_value between 50 and 0 then 'low_end_product'
            else 'null'
            end as class_of_product
    from sales
)

select * from final