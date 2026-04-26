with annual_revenue as (
    select
        extract(year from payment_date) as stat_year,
        sum(net_receipt_amount) as annual_revenue_amount
    from {{ ref('int_finance__receipt_transactions') }}
    where payment_date is not null
    group by 1
),

latest_year as (
    select max(stat_year) as stat_year
    from annual_revenue
),

current_year as (
    select
        r.stat_year,
        r.annual_revenue_amount as current_value
    from annual_revenue r
    inner join latest_year y
        on r.stat_year = y.stat_year
),

previous_year as (
    select
        c.stat_year,
        coalesce(p.annual_revenue_amount, 0) as previous_value
    from current_year c
    left join annual_revenue p
        on p.stat_year = c.stat_year - 1
)

select
    c.stat_year,
    cast(concat(cast(c.stat_year as string), '-01-01') as date) as stat_date,
    c.current_value,
    p.previous_value,
    case
        when p.previous_value = 0 then null
        else ((c.current_value - p.previous_value) / p.previous_value) * 100.0
    end as yoy_pct,
    case
        when c.current_value > p.previous_value then 'up'
        when c.current_value < p.previous_value then 'down'
        else 'flat'
    end as trend_direction,
    c.current_value / 1000000000.0 as current_value_billion
from current_year c
inner join previous_year p
    on c.stat_year = p.stat_year
