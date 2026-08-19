{{
  config(
    materialized='table'
  )
}}

with monthly_net_inflow as (
    select
        extract(year from txn_date) as txn_year
        , extract(month from txn_date) as txn_month_num
        , (extract(year from txn_date) - 2025) * 12
            + extract(month from txn_date) as month_index
        , sum(
            case when transaction_type = 'DEPOSIT' then amount
                 when transaction_type = 'WITHDRAWAL' then -amount
                 else 0
            end
        ) as net_inflow
    from {{ ref('stg_aptimus_transactions') }}
    where txn_date <= '2026-06-30'
    group by 1, 2, 3

)

, regression_inputs as (

    select
        count(*) as n
        , sum(month_index) as sum_x
        , sum(net_inflow) as sum_y
        , sum(month_index * net_inflow) as sum_xy
        , sum(month_index * month_index) as sum_x2
    from monthly_net_inflow

)

--linear regression method
, regression as (
    select
        safe_divide(
            n * sum_xy - sum_x * sum_y,
            n * sum_x2 - sum_x * sum_x
        ) as slope
        , safe_divide(
            sum_y - safe_divide(n * sum_xy - sum_x * sum_y, n * sum_x2 - sum_x * sum_x) * sum_x,
            n
        ) as intercept
    from regression_inputs

)

, forecast_months as (
    select 19 as month_index, 2026 as txn_year, 7 as txn_month_num, 'Jul 2026' as month_label
    union all
    select 20, 2026, 8, 'Aug 2026'
    union all
    select 21, 2026, 9, 'Sep 2026'
)

-- actuals: Jan 2025 - Jun 2026
select
    concat(
        format_date('%b', date(txn_year, txn_month_num, 1)), ' ', cast(txn_year as string)
    ) as month_label
    , month_index
    , net_inflow as amount
    , 'actual' as type
from monthly_net_inflow

union all

-- forecast: Jul - Sep 2026
select
    f.month_label
    , f.month_index
    , r.intercept + r.slope * f.month_index as amount
    , 'forecast' as type
from forecast_months as f
cross join regression as r
order by month_index