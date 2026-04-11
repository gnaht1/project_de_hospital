with monthly_stats as (
    -- Aggregate total visits and return visits by month
    select
        extract(year from admission_time) as stat_year,
        extract(month from admission_time) as stat_month,
        
        -- Create a proper Date column for Superset Time Series / Trendlines
        date_trunc('month', cast(admission_time as timestamp)) as stat_date,
        
        count(dot_dieu_tri_id) as total_visits,
        sum(is_return_patient) as return_visits
    from {{ ref('int_clinical__patient_visits_classified') }}
    group by 1, 2, 3
),

rate_calculation as (
    select
        stat_year,
        stat_month,
        stat_date,
        total_visits,
        return_visits,
        
        -- Calculate Re-examination Rate (Avoid division by zero)
        -- Result will be a decimal like 0.325 (which is 32.5%)
        case 
            when total_visits > 0 then cast(return_visits as double) / total_visits 
            else 0 
        end as current_rate
    from monthly_stats
),

lagged_rates as (
    select
        stat_year,
        stat_month,
        stat_date,
        'T' || lpad(cast(stat_month as string), 2, '0') as month_label,
        
        current_rate,
        
        -- Fetch the rate from the previous month for trend calculation
        lag(current_rate) over (order by stat_year, stat_month) as last_month_rate
        
    from rate_calculation
)

select
    *,
    -- Calculate percentage point difference (e.g., +0.021 means +2.1%)
    current_rate - last_month_rate as trend_diff
from lagged_rates