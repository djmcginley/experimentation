-- The following SQL query is an example of how I'd model experiment data to monitor results in a downstream BI tool like Tableau or Mode

-- Begin by creating row level data for each experiment cohort for each key date milestone (ex: days since entered experiment = 0,8,14,30,60,90,etc.)
-- Each row contains an aggregated snapshot for that cohort at each date milestone
with aggregates AS
(
SELECT
  e.cohort
  ,e.start_date
  ,k.user_platform
  ,k.days_since_reg
  ,count(distinct k.customer_id) as registrations
  ,count(case when k.started_subscription then k.customer_id end) as started_subscription
  ,count(case when k.trial_type = 'Trial' then k.customer_id end) as trials_started
  ,count(case when k.total_payments > 0 then k.customer_id end) as total_payers
  ,count(case when k.subscription_payments > 0 then k.customer_id end) as sub_payers
  ,count(case when k.annual_subscription_payments > 0 then k.customer_id end) as annual_sub_payers
  ,count(case when k.monthly_subscription_payments > 0 then k.customer_id end) as monthly_sub_payers
  ,count(case when k.trial_type = 'Trial' and k.subscription_payments > 0 then k.customer_id end) as trial_converts  
  ,sum(k.total_payments) as total_payments
  ,sum(k.subscription_payments) as subscription_payments
  ,sum(k.annual_subscription_payments) as annual_subscription_payments
  ,sum(k.monthly_subscription_payments) as monthly_subscription_payments
  ,sum(k.credit_payments) as credit_payments
  ,count(case when k.had_paid_sub then k.customer_id end) as active_paid_subs
  ,count(case when k.had_paid_sub and k.sub_sku LIKE '%year%' then k.customer_id end) as active_paid_annual_subs
  ,count(case when k.had_paid_sub and k.sub_sku LIKE '%month%' then k.customer_id end) as active_paid_monthly_subs
  ,sum(k.mrr) as total_mrr
  ,sum(case when k.sub_sku LIKE '%year%' then k.mrr end) as annual_mrr
  ,sum(case when k.sub_sku LIKE '%month%' then k.mrr end) as monthly_mrr
FROM user_key_dates k  -- table that creates a row for each date milestone for eaach customer that represents a snapshot of their metrics at that time
  JOIN experiment_cohorts e -- table that splits users into each experiment group (treatment vs control)
    ON k.customer_id = e.customer_id
WHERE 1=1
      AND k.registration_at >= e.start_date
      and k.registration_at between '2023-01-01' and '2023-04-30' --  Limit to experiment dates only
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
)

-- We'll join in forecast data models so we can also understand average predicted LTV for each cohort at each date milestone
,user_forecasts AS
(
  SELECT
      e.cohort
    , e.start_date
    , l.platform as user_platform
    , SUM(CASE WHEN l.month BETWEEN 0 AND 12 THEN l.forecasted_payments ELSE 0 END) AS one_year_forecasted_payments   --  12 month forecasted payments
    , SUM(CASE WHEN l.month BETWEEN 0 AND 24 THEN l.forecasted_payments ELSE 0 END) AS two_year_forecasted_payments   --  2 years forecasted payments
  FROM user_ltv_forecasts l -- data model that predicts a users future value at each date milestone
    JOIN experiment_cohorts e 
      ON l.customer_id = e.customer_id
  WHERE 1=1
        AND l.registration_at >= e.start_date
        and l.registration_at between '2022-08-02' and '2022-08-28' --  Limit to experiment dates only
        AND l.month <= 24
  GROUP BY 1,2,3
)

-- Finally, we'll pre-calculate the CRV and ARPU, along with all other metrics, to make Tableau charts easier to read
SELECT
  a.*
  ,CAST(a.had_outbound_activity AS FLOAT) / a.registrations as outbound_activity_CVR
  ,CAST(a.started_subscription AS FLOAT) / a.registrations as started_subscription_CVR
  ,CAST(a.trials_started AS FLOAT) / a.registrations as started_trial_CVR
  ,case when a.trials_started = 0 then NULL else CAST(a.trial_converts AS FLOAT) / a.trials_started end as trial_CVR
  ,CAST(a.total_payers AS FLOAT) / a.registrations as payer_CVR
  ,CAST(a.sub_payers AS FLOAT) / a.registrations as sub_payer_CVR
  ,CAST(a.annual_sub_payers AS FLOAT) / a.registrations as annual_sub_payer_CVR
  ,CAST(a.monthly_sub_payers AS FLOAT) / a.registrations as monthly_sub_payer_CVR
  ,CAST(a.credit_payers AS FLOAT) / a.registrations as credit_payer_CVR
  ,CAST(a.credit_only_payers AS FLOAT) / a.registrations as credit_only_payer_CVR
  ,a.total_payments / a.registrations as ARPU
  ,a.subscription_payments / a.registrations as sub_ARPU
  ,a.credit_payments / a.registrations as credit_ARPU
  ,CAST(a.active_paid_subs AS FLOAT) / a.registrations as active_paid_sub_rate
  ,CAST(a.active_paid_annual_subs AS FLOAT) / a.registrations as active_paid_annual_sub_rate
  ,CAST(a.active_paid_monthly_subs AS FLOAT) / a.registrations as active_paid_monthly_sub_rate
  ,a.total_mrr / a.registrations as MRR_per_user
  ,a.annual_mrr / a.registrations as annual_MRR_per_user
  ,a.monthly_mrr / a.registrations as monthly_MRR_per_user
  ,f.one_year_forecasted_payments
  ,f.two_year_forecasted_payments
FROM aggregates a
  LEFT JOIN user_forecasts f 
    ON a.cohort = f.cohort 
    AND a.start_date = f.start_date 
    AND a.user_platform = f.user_platform

