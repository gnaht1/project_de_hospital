with annual_visits as (
    select
        extract(year from visit_date) as stat_year,
        count(distinct dot_dieu_tri_id) as annual_visit_count
    from {{ ref('int_clinical__patient_visits_classified') }}
    where visit_date is not null
    group by 1
),

latest_year as (
    select max(stat_year) as stat_year
    from annual_visits
),

current_year as (
    select
        v.stat_year,
        v.annual_visit_count as current_value
    from annual_visits v
    inner join latest_year y
        on v.stat_year = y.stat_year
),

previous_year as (
    select
        c.stat_year,
        coalesce(p.annual_visit_count, 0) as previous_value
    from current_year c
    left join annual_visits p
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
    end as trend_direction
from current_year c
inner join previous_year p
    on c.stat_year = p.stat_year
