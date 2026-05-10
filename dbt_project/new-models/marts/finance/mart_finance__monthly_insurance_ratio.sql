with revenue_transactions as (
    select * from {{ ref('int_finance__revenue_transactions') }}
),

monthly_revenue as (
    -- Aggregate total amounts by month
    select
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        date_trunc('month', cast(order_time as timestamp)) as stat_date,
        
        sum(insurance_amount) as total_insurance_payout,
        sum(total_amount) as total_revenue
        
    from revenue_transactions
    group by 1, 2, 3
),

ratio_calculation as (
    -- Calculate ratio
    select
        stat_year,
        stat_month,
        stat_date,
        total_insurance_payout,
        total_revenue,
        
        -- Prevent division by zero
        case 
            when total_revenue > 0 then cast(total_insurance_payout as double) / total_revenue 
            else 0 
        end as insurance_ratio
    from monthly_revenue
),

lagged_ratios as (
    select
        stat_year,
        stat_month,
        stat_date,
        'T' || lpad(cast(stat_month as string), 2, '0') as month_label,
        
        insurance_ratio,
        
        -- Fetch the rate from the previous month for trend calculation
        lag(insurance_ratio) over (order by stat_year, stat_month) as last_month_ratio
        
    from ratio_calculation
)

select
    *,
    insurance_ratio - last_month_ratio as trend_diff
from lagged_ratios