{{
  config(
    materialized='table',
    description='campaign attribution linking campaigns to customer signups and transaction revenue with full roi analysis'
  )
}}

with campaigns as (

  select
    campaign_id,
    campaign_name,
    channel,
    country_code,
    campaign_start_date,
    campaign_end_date,
    ad_spend,
    impressions,
    clicks,
    conversions,
    
    -- campaign duration
    date_diff(campaign_end_date, campaign_start_date, day) as campaign_duration_days,
    
    -- basic campaign metrics
    case
      when impressions > 0
        then round(clicks * 100.0 / impressions, 2)
      else 0
    end as click_through_rate_pct,
    
    case
      when clicks > 0
        then round(conversions * 100.0 / clicks, 2)
      else 0
    end as conversion_rate_pct,
    
    case
      when conversions > 0
        then round(ad_spend / conversions, 2)
      else 0
    end as cost_per_conversion,
    
    case
      when clicks > 0
        then round(ad_spend / clicks, 2)
      else 0
    end as cost_per_click,
    
    case
      when impressions > 0
        then round(ad_spend / impressions * 1000, 2)
      else 0
    end as cost_per_thousand_impressions

  from {{ ref('stg_transfera_marketing_campaigns') }}

),

attributed_customers as (

  select
    c.customer_id,
    c.full_name,
    c.email,
    c.customer_segment,
    c.kyc_status,
    c.signup_date,
    c.country_code,
    
    -- find matching campaign (signed up during campaign period in same country)
    mc.campaign_id,
    mc.campaign_name,
    mc.channel,
    mc.campaign_start_date,
    mc.campaign_end_date,
    
    -- time to signup from campaign start
    date_diff(c.signup_date, mc.campaign_start_date, day) as days_from_campaign_start_to_signup,
    
    -- customer tenure
    date_diff(current_date(), c.signup_date, day) as days_since_signup

  from {{ ref('stg_transfera_customers') }} as c
  inner join {{ ref('stg_transfera_marketing_campaigns') }} as mc
    on c.country_code = mc.country_code
    and c.signup_date between mc.campaign_start_date and mc.campaign_end_date

),

customer_transaction_summary as (

  select
    t.customer_id,
    
    -- all transactions
    count(distinct t.transaction_id) as total_transactions,
    count(distinct case when t.transaction_status = 'completed' then t.transaction_id end) as completed_transactions,
    count(distinct case when t.transaction_status = 'failed' then t.transaction_id end) as failed_transactions,
    
    -- revenue metrics (completed only)
    sum(case when t.transaction_status = 'completed' then t.transaction_amount else 0 end) as total_revenue,
    sum(case when t.transaction_status = 'completed' then t.transaction_fee else 0 end) as total_fees,
    avg(case when t.transaction_status = 'completed' then t.transaction_amount end) as avg_transaction_amount,
    
    -- temporal metrics
    min(t.transaction_datetime) as first_transaction_date,
    max(case when t.transaction_status = 'completed' then t.transaction_datetime end) as last_completed_transaction_date,
    
    -- time to first transaction
    min(date(t.transaction_datetime)) as first_transaction_date_only,
    
    -- recency
    date_diff(current_date(), date(max(case when t.transaction_status = 'completed' then t.transaction_datetime end)), day) as days_since_last_transaction,
    
    -- activity
    count(distinct date(t.transaction_datetime)) as active_transaction_days,
    count(distinct format_date('%Y-%m', t.transaction_datetime)) as active_months

  from {{ ref('stg_transfera_transactions') }} as t
  group by t.customer_id

),

campaign_customer_metrics as (

  select
    ac.campaign_id,
    ac.campaign_name,
    ac.channel,
    ac.campaign_start_date,
    ac.campaign_end_date,
    ac.country_code,
    
    ac.customer_id,
    ac.full_name,
    ac.email,
    ac.customer_segment,
    ac.kyc_status,
    ac.signup_date,
    ac.days_from_campaign_start_to_signup,
    ac.days_since_signup,
    
    -- transaction metrics
    coalesce(cts.total_transactions, 0) as total_transactions,
    coalesce(cts.completed_transactions, 0) as completed_transactions,
    coalesce(cts.failed_transactions, 0) as failed_transactions,
    coalesce(cts.total_revenue, 0) as customer_lifetime_value,
    coalesce(cts.total_fees, 0) as customer_lifetime_fees,
    coalesce(cts.avg_transaction_amount, 0) as avg_transaction_amount,
    
    cts.first_transaction_date,
    cts.last_completed_transaction_date,
    cts.first_transaction_date_only,
    cts.days_since_last_transaction,
    coalesce(cts.active_transaction_days, 0) as active_transaction_days,
    coalesce(cts.active_months, 0) as active_months,
    
    -- time to first transaction
    case
      when cts.first_transaction_date_only is not null
        then date_diff(cts.first_transaction_date_only, ac.signup_date, day)
      else null
    end as days_to_first_transaction,
    
    -- customer status
    case
      when cts.completed_transactions = 0 then 'never_transacted'
      when cts.days_since_last_transaction <= 30 then 'active'
      when cts.days_since_last_transaction <= 90 then 'at_risk'
      else 'churned'
    end as customer_status,
    
    -- customer value tier
    case
      when coalesce(cts.total_revenue, 0) >= 10000 then 'high_value'
      when coalesce(cts.total_revenue, 0) >= 5000 then 'medium_value'
      when coalesce(cts.total_revenue, 0) >= 1000 then 'low_value'
      when coalesce(cts.total_revenue, 0) > 0 then 'minimal_value'
      else 'no_value'
    end as value_tier,
    
    -- activation flag
    case
      when cts.completed_transactions >= 1 then true
      else false
    end as is_activated,
    
    -- retention flag
    case
      when cts.completed_transactions > 1 and cts.days_since_last_transaction <= 90 then true
      else false
    end as is_retained

  from attributed_customers as ac
  left join customer_transaction_summary as cts
    on ac.customer_id = cts.customer_id

),

campaign_aggregated_metrics as (

  select
    ccm.campaign_id,
    ccm.campaign_name,
    ccm.channel,
    ccm.campaign_start_date,
    ccm.campaign_end_date,
    ccm.country_code,
    
    -- customer counts
    count(distinct ccm.customer_id) as attributed_customers,
    count(distinct case when ccm.is_activated then ccm.customer_id end) as activated_customers,
    count(distinct case when ccm.is_retained then ccm.customer_id end) as retained_customers,
    
    -- customer segment breakdown
    count(distinct case when ccm.customer_segment = 'SME' then ccm.customer_id end) as sme_customers,
    count(distinct case when ccm.customer_segment = 'Retail' then ccm.customer_id end) as retail_customers,
    count(distinct case when ccm.customer_segment = 'Enterprise' then ccm.customer_id end) as enterprise_customers,
    
    -- value tier breakdown
    count(distinct case when ccm.value_tier = 'high_value' then ccm.customer_id end) as high_value_customers,
    count(distinct case when ccm.value_tier = 'medium_value' then ccm.customer_id end) as medium_value_customers,
    count(distinct case when ccm.value_tier = 'low_value' then ccm.customer_id end) as low_value_customers,
    
    -- status breakdown
    count(distinct case when ccm.customer_status = 'active' then ccm.customer_id end) as active_customers,
    count(distinct case when ccm.customer_status = 'at_risk' then ccm.customer_id end) as at_risk_customers,
    count(distinct case when ccm.customer_status = 'churned' then ccm.customer_id end) as churned_customers,
    count(distinct case when ccm.customer_status = 'never_transacted' then ccm.customer_id end) as never_transacted_customers,
    
    -- revenue metrics
    sum(ccm.customer_lifetime_value) as total_attributed_revenue,
    sum(ccm.customer_lifetime_fees) as total_attributed_fees,
    avg(ccm.customer_lifetime_value) as avg_revenue_per_customer,
    
    -- transaction metrics
    sum(ccm.completed_transactions) as total_completed_transactions,
    sum(ccm.failed_transactions) as total_failed_transactions,
    avg(ccm.completed_transactions) as avg_transactions_per_customer,
    
    -- time to value metrics
    avg(ccm.days_to_first_transaction) as avg_days_to_first_transaction,
    avg(case when ccm.is_activated then ccm.days_to_first_transaction end) as avg_days_to_first_transaction_activated_only,
    
    -- activation and retention rates
    case
      when count(distinct ccm.customer_id) > 0
        then round(count(distinct case when ccm.is_activated then ccm.customer_id end) * 100.0 / count(distinct ccm.customer_id), 2)
      else 0
    end as activation_rate_pct,
    
    case
      when count(distinct case when ccm.is_activated then ccm.customer_id end) > 0
        then round(count(distinct case when ccm.is_retained then ccm.customer_id end) * 100.0 / count(distinct case when ccm.is_activated then ccm.customer_id end), 2)
      else 0
    end as retention_rate_pct

  from campaign_customer_metrics as ccm
  group by
    ccm.campaign_id,
    ccm.campaign_name,
    ccm.channel,
    ccm.campaign_start_date,
    ccm.campaign_end_date,
    ccm.country_code

),

final_campaign_roi as (

  select
    c.campaign_id,
    c.campaign_name,
    c.channel,
    c.country_code,
    c.campaign_start_date,
    c.campaign_end_date,
    c.campaign_duration_days,
    
    -- campaign spend and metrics
    c.ad_spend,
    c.impressions,
    c.clicks,
    c.conversions,
    c.click_through_rate_pct,
    c.conversion_rate_pct,
    c.cost_per_conversion,
    c.cost_per_click,
    c.cost_per_thousand_impressions,
    
    -- attributed customer counts
    coalesce(cam.attributed_customers, 0) as attributed_customers,
    coalesce(cam.activated_customers, 0) as activated_customers,
    coalesce(cam.retained_customers, 0) as retained_customers,
    
    -- segment breakdown
    coalesce(cam.sme_customers, 0) as sme_customers,
    coalesce(cam.retail_customers, 0) as retail_customers,
    coalesce(cam.enterprise_customers, 0) as enterprise_customers,
    
    -- value tier breakdown
    coalesce(cam.high_value_customers, 0) as high_value_customers,
    coalesce(cam.medium_value_customers, 0) as medium_value_customers,
    coalesce(cam.low_value_customers, 0) as low_value_customers,
    
    -- status breakdown
    coalesce(cam.active_customers, 0) as active_customers,
    coalesce(cam.at_risk_customers, 0) as at_risk_customers,
    coalesce(cam.churned_customers, 0) as churned_customers,
    coalesce(cam.never_transacted_customers, 0) as never_transacted_customers,
    
    -- revenue metrics
    coalesce(cam.total_attributed_revenue, 0) as total_attributed_revenue,
    coalesce(cam.total_attributed_fees, 0) as total_attributed_fees,
    coalesce(cam.total_attributed_revenue, 0) + coalesce(cam.total_attributed_fees, 0) as total_gross_revenue,
    coalesce(cam.avg_revenue_per_customer, 0) as avg_revenue_per_customer,
    
    -- transaction metrics
    coalesce(cam.total_completed_transactions, 0) as total_completed_transactions,
    coalesce(cam.total_failed_transactions, 0) as total_failed_transactions,
    coalesce(cam.avg_transactions_per_customer, 0) as avg_transactions_per_customer,
    
    -- time to value
    cam.avg_days_to_first_transaction,
    cam.avg_days_to_first_transaction_activated_only,
    
    -- rates
    coalesce(cam.activation_rate_pct, 0) as activation_rate_pct,
    coalesce(cam.retention_rate_pct, 0) as retention_rate_pct,
    
    -- roi calculations
    case
      when coalesce(cam.attributed_customers, 0) > 0
        then round(c.ad_spend / cam.attributed_customers, 2)
      else 0
    end as cost_per_acquisition,
    
    case
      when coalesce(cam.activated_customers, 0) > 0
        then round(c.ad_spend / cam.activated_customers, 2)
      else 0
    end as cost_per_activated_customer,
    
    case
      when c.ad_spend > 0
        then round((coalesce(cam.total_attributed_revenue, 0) + coalesce(cam.total_attributed_fees, 0)) / c.ad_spend, 2)
      else 0
    end as return_on_ad_spend,
    
    case
      when c.ad_spend > 0
        then round((coalesce(cam.total_attributed_revenue, 0) + coalesce(cam.total_attributed_fees, 0)) - c.ad_spend, 2)
      else 0
    end as net_profit,
    
    case
      when c.ad_spend > 0
        then round(((coalesce(cam.total_attributed_revenue, 0) + coalesce(cam.total_attributed_fees, 0)) - c.ad_spend) / c.ad_spend * 100, 2)
      else 0
    end as roi_percentage,
    
    -- efficiency score (0-100)
    least(100, round(
      (case when cam.activation_rate_pct >= 50 then 25 when cam.activation_rate_pct >= 30 then 15 when cam.activation_rate_pct >= 10 then 5 else 0 end) +
      (case when cam.retention_rate_pct >= 50 then 25 when cam.retention_rate_pct >= 30 then 15 when cam.retention_rate_pct >= 10 then 5 else 0 end) +
      (case 
        when c.ad_spend > 0 and (coalesce(cam.total_attributed_revenue, 0) + coalesce(cam.total_attributed_fees, 0)) / c.ad_spend >= 5 then 25
        when c.ad_spend > 0 and (coalesce(cam.total_attributed_revenue, 0) + coalesce(cam.total_attributed_fees, 0)) / c.ad_spend >= 3 then 15
        when c.ad_spend > 0 and (coalesce(cam.total_attributed_revenue, 0) + coalesce(cam.total_attributed_fees, 0)) / c.ad_spend >= 1 then 5
        else 0
      end) +
      (case when coalesce(cam.high_value_customers, 0) * 100.0 / nullif(coalesce(cam.attributed_customers, 0), 0) >= 20 then 25 when coalesce(cam.high_value_customers, 0) * 100.0 / nullif(coalesce(cam.attributed_customers, 0), 0) >= 10 then 15 else 5 end)
    , 0)) as campaign_efficiency_score,
    
    -- campaign performance tier
    case
      when c.ad_spend > 0 and (coalesce(cam.total_attributed_revenue, 0) + coalesce(cam.total_attributed_fees, 0)) / c.ad_spend >= 5
        then 'excellent'
      when c.ad_spend > 0 and (coalesce(cam.total_attributed_revenue, 0) + coalesce(cam.total_attributed_fees, 0)) / c.ad_spend >= 3
        then 'good'
      when c.ad_spend > 0 and (coalesce(cam.total_attributed_revenue, 0) + coalesce(cam.total_attributed_fees, 0)) / c.ad_spend >= 1
        then 'break_even'
      else 'poor'
    end as performance_tier,
    
    -- recommendation
    case
      when c.ad_spend > 0 and (coalesce(cam.total_attributed_revenue, 0) + coalesce(cam.total_attributed_fees, 0)) / c.ad_spend >= 5
        and coalesce(cam.activation_rate_pct, 0) >= 40
        then 'scale_up'
      when c.ad_spend > 0 and (coalesce(cam.total_attributed_revenue, 0) + coalesce(cam.total_attributed_fees, 0)) / c.ad_spend >= 3
        then 'maintain'
      when c.ad_spend > 0 and (coalesce(cam.total_attributed_revenue, 0) + coalesce(cam.total_attributed_fees, 0)) / c.ad_spend >= 1
        then 'optimize'
      else 'pause_or_stop'
    end as campaign_recommendation,
    
    current_timestamp() as analysis_timestamp

  from campaigns as c
  left join campaign_aggregated_metrics as cam
    on c.campaign_id = cam.campaign_id

)

select * from final_campaign_roi
order by return_on_ad_spend desc, total_attributed_revenue desc
