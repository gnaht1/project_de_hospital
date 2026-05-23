with actuals as (
    select *
    from {{ ref('int_finance__monthly_revenue_actual') }}
),

targets as (
    select
        stat_date,
        stat_year,
        stat_month,
        case
            when stat_month in (1, 2, 3) then 2.5
            when stat_month in (4, 5, 6) then 2.8
            when stat_month in (7, 8, 9) then 3.0
            else 3.0
        end as target_revenue_billion
    from actuals
)

select
    a.stat_date,
    a.stat_year,
    a.stat_month,
    a.month_label,
    a.actual_revenue_billion as doanh_thu_ty,
    t.target_revenue_billion as muc_tieu_ty,
    case
        when t.target_revenue_billion = 0 then null
        else (a.actual_revenue_billion / t.target_revenue_billion) * 100.0
    end as target_achievement_pct
from actuals a
left join targets t
    on a.stat_date = t.stat_date
order by a.stat_date
