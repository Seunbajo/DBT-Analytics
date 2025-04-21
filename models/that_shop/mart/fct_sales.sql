{{
    config(
        materialized = 'incremental'
        , tags = ['that_shop']
    )
}}

with sales as (
    select * from {{ ref('stg_sales') }}
)

--Sales model to tell the good bought

, final as (
    select
        * 
        , case 
            when total_value >= 300 then 'highest_price_product'
            when total_value >= 200 then 'high_price_product'
            when total_value >= 100 then 'middle_price_product'
            when total_value >= 50 then 'low_price_product'
            else 'very_low_price_product'
          end as class_of_product
    from sales
)

select * from final