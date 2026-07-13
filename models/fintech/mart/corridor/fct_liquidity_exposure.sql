{{
  config(
    materialized='table'
  )
}}

with latest_balance as (
    select 
        partner,
        closing_balance as current_balance,
        date as balance_date
    from {{ ref('stg_corridor_partner_wallet_balance') }}
    qualify row_number() over (
        partition by partner 
        order by date desc
    ) = 1
),

recent_outflow_stats as (
    select 
        partner,
        round(avg(total_outflow_usd), 2) as avg_daily_outflow,
        round(stddev(total_outflow_usd), 2) as stddev_daily_outflow,
        round(min(total_outflow_usd), 2) as min_daily_outflow,
        round(max(total_outflow_usd), 2) as max_daily_outflow,
        count(distinct date) as days_in_period
    from {{ ref('stg_corridor_partner_outflow') }}
    where date >= date_sub(
        (
            select max(date)
            from {{ ref('stg_corridor_partner_outflow') }}
        ),
        interval 30 day
    )
    group by partner
),

projections as (
    select 
        lb.partner,
        lb.current_balance,
        lb.balance_date,
        ro.avg_daily_outflow,
        ro.stddev_daily_outflow,
        ro.max_daily_outflow,
        ro.min_daily_outflow,
        round(lb.current_balance / nullif(ro.avg_daily_outflow, 0), 1) as days_of_coverage,

        -- conservative projection: use avg + 1 std dev to account for volatility
        round(
            ro.avg_daily_outflow + coalesce(ro.stddev_daily_outflow, 0),
            2
        ) as conservative_daily_outflow,

        -- day 1 projection
        round(
            lb.current_balance - (ro.avg_daily_outflow * 1),
            2
        ) as projected_balance_day1,

        round(
            lb.current_balance - (
                (ro.avg_daily_outflow + coalesce(ro.stddev_daily_outflow, 0)) * 1
            ),
            2
        ) as conservative_balance_day1,

        -- day 2 projection
        round(
            lb.current_balance - (ro.avg_daily_outflow * 2),
            2
        ) as projected_balance_day2,

        round(
            lb.current_balance - (
                (ro.avg_daily_outflow + coalesce(ro.stddev_daily_outflow, 0)) * 2
            ),
            2
        ) as conservative_balance_day2,

        -- day 3 projection
        round(
            lb.current_balance - (ro.avg_daily_outflow * 3),
            2
        ) as projected_balance_day3,

        round(
            lb.current_balance - (
                (ro.avg_daily_outflow + coalesce(ro.stddev_daily_outflow, 0)) * 3
            ),
            2
        ) as conservative_balance_day3,

        -- recommended buffer: 5 days of conservative outflow
        round(
            (ro.avg_daily_outflow + coalesce(ro.stddev_daily_outflow, 0)) * 5,
            2
        ) as recommended_buffer_5days,

        -- current days of coverage
        round(
            lb.current_balance / nullif(ro.avg_daily_outflow, 0),
            1
        ) as current_days_coverage
    from latest_balance lb
    left join recent_outflow_stats ro
        on lb.partner = ro.partner
)

select 
    partner,
    balance_date,
    days_of_coverage,
    current_balance,
    avg_daily_outflow,
    min_daily_outflow,
    max_daily_outflow,
    stddev_daily_outflow, 
    conservative_daily_outflow,
    current_days_coverage,
    projected_balance_day1,
    conservative_balance_day1,
    projected_balance_day2,
    conservative_balance_day2,
    projected_balance_day3,
    conservative_balance_day3,
    recommended_buffer_5days,
    case 
        when conservative_balance_day1 <= 0 then 'CRITICAL - Day 1'
        when conservative_balance_day2 <= 0 then 'HIGH RISK - Day 2'
        when conservative_balance_day3 <= 0 then 'MEDIUM RISK - Day 3'
        when current_balance < recommended_buffer_5days then 'LOW RISK - Below Buffer'
        else 'HEALTHY'
    end as risk_level,
    round(
        recommended_buffer_5days - current_balance,
        2
    ) as recommended_topup_amount
from projections
order by 
    case 
        when conservative_balance_day1 <= 0 then 1
        when conservative_balance_day2 <= 0 then 2
        when conservative_balance_day3 <= 0 then 3
        when current_balance < recommended_buffer_5days then 4
        else 5
    end,
    conservative_balance_day3 asc