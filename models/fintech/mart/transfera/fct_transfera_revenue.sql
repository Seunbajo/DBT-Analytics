{{
  config(
    materialized='table',
    description='Comprehensive revenue KPIs and metrics for executive dashboard'
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

, final as (
    select
        t.transaction_id,
        t.customer_id,
        t.transaction_amount,
        t.transaction_fee,
        t.transaction_datetime,
        t.payment_method,
        t.source_country,
        t.destination_country,
        t.transaction_status,
        c.customer_segment,
        c.signup_date,
        sc.country_name as source_country_name,
        dc.country_name as destination_country_name,
        date(t.transaction_datetime) as transaction_date,
        format_date('%y-%m', date(t.transaction_datetime)) as transaction_month,
        format_date('%y-q%q', date(t.transaction_datetime)) as transaction_quarter,
        extract(year from t.transaction_datetime) as transaction_year,
        extract(month from t.transaction_datetime) as month_number,
        format_date('%b', date(t.transaction_datetime)) as month_name,
        extract(dayofweek from t.transaction_datetime) as day_of_week,
        concat(
            t.source_country,
            ' → ',
            t.destination_country
        ) as corridor
    from transactions as t
    left join customers as c
        on t.customer_id = c.customer_id
    left join countries as sc
        on t.source_country = sc.country_code
    left join countries as dc
        on t.destination_country = dc.country_code
    where t.transaction_status = 'Successful'
)

select * from final
