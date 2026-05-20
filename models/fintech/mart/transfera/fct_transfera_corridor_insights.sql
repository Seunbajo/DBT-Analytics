{{
    config(
        materialized = 'table',
        description = 'revenue by corridor analysis with transaction-level financial and customer insights'
    )
}}

with transactions as (
    select * from {{ ref('stg_transfera_transactions') }}
)

, countries as (
    select * from {{ ref('stg_transfera_countries') }}
)

, customers as (
    select * from {{ ref('stg_transfera_customers') }}
)

, corridor_transactions as (
    select
        t.transaction_id,
        t.customer_id,
        -- transaction metrics
        t.transaction_amount,
        t.transaction_fee,
        t.transaction_amount + t.transaction_fee as gross_revenue,
        t.transaction_datetime,
        date(t.transaction_datetime) as transaction_date,
        format_date(
            '%y-%m',
            date(t.transaction_datetime)
        ) as transaction_month,
        format_date(
            '%y-q%q',
            date(t.transaction_datetime)
        ) as transaction_quarter,
        extract(year from t.transaction_datetime) as transaction_year,
        format_date(
            '%b',
            date(t.transaction_datetime)
        ) as month_name,
        extract(month from t.transaction_datetime) as month_number,
        extract(dayofweek from t.transaction_datetime) as day_of_week,
        format_date(
            '%a',
            date(t.transaction_datetime)
        ) as day_name,
        -- payment details
        t.payment_method,
        -- corridor information
        t.source_country,
        t.destination_country,
        sc.country_name as source_country_name,
        dc.country_name as destination_country_name,
        concat(
            sc.country_name,
            ' → ',
            dc.country_name
        ) as corridor_name,
        concat(
            t.source_country,
            '-',
            t.destination_country
        ) as corridor_code,
        -- customer information
        c.customer_segment,
        c.kyc_status,
        c.signup_date,
        date_diff(
            date(t.transaction_datetime),
            c.signup_date,
            day
        ) as days_since_signup,
        -- fee calculations
        safe_divide(
            t.transaction_fee,
            nullif(t.transaction_amount, 0)
        ) * 100 as fee_percentage,
        -- transaction amount tiers
        case
            when t.transaction_amount < 100 then '< $100'
            when t.transaction_amount < 500 then '$100-$500'
            when t.transaction_amount < 1000 then '$500-$1k'
            when t.transaction_amount < 5000 then '$1k-$5k'
            when t.transaction_amount < 10000 then '$5k-$10k'
            else '$10k+'
        end as amount_tier,
        -- transaction value classification
        case
            when t.transaction_amount >= 5000 then 'high value transaction'
            when t.transaction_amount >= 1000 then 'medium value transaction'
            else 'low value transaction'
        end as transaction_value_tier,
        current_timestamp() as analysis_timestamp
    from transactions as t
    left join customers as c
        on t.customer_id = c.customer_id
    left join countries as sc
        on t.source_country = sc.country_code
    left join countries as dc
        on t.destination_country = dc.country_code
)

, final as (
    select * from corridor_transactions
    order by transaction_datetime desc
) 

select * from final
