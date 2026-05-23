with monthly_revenue as (
    select
        cast(date_trunc('month', thoi_gian_thanh_toan) as date) as stat_date,
        year(thoi_gian_thanh_toan) as stat_year,
        month(thoi_gian_thanh_toan) as stat_month,
        sum(thanh_tien) as actual_revenue_amount
    from {{ ref('int_finance__valid_payments') }}
    group by 1, 2, 3
)

select
    stat_date,
    stat_year,
    stat_month,
    concat('T', lpad(cast(stat_month as string), 2, '0')) as month_label,
    actual_revenue_amount,
    actual_revenue_amount / 1000000000.0 as actual_revenue_billion
from monthly_revenue
