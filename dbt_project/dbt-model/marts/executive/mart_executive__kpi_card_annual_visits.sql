with monthly_traffic as (
    select * from {{ ref('mart_clinical__monthly_patient_traffic') }}
),

monthly_visits as (
    select
        stat_year,
        stat_month,
        cast(stat_date as date) as stat_date,
        total_visits as current_value
    from monthly_traffic
),

lagged_visits as (
    select
        stat_year,
        stat_month,
        stat_date,
        current_value,
        lag(current_value) over (order by stat_date) as previous_value
    from monthly_visits
)

select
    stat_year,
    stat_month,
    stat_date,
    current_value,
    previous_value,
    case
        when previous_value is null or previous_value = 0 then null
        else ((current_value - previous_value) / previous_value) * 100.0
    end as mom_pct,
    case
        when previous_value is null then null
        when current_value > previous_value then 'up'
        when current_value < previous_value then 'down'
        else 'flat'
    end as trend_direction
from lagged_visits
