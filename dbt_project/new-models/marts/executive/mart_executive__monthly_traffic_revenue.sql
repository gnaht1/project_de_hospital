with date_window_options as (
    select 'TODAY' as date_window_key, 'Hôm nay' as date_window_label, 1 as window_days, 1 as sort_order
    union all
    select 'LAST_7_DAYS' as date_window_key, '7 ngày' as date_window_label, 7 as window_days, 2 as sort_order
    union all
    select 'LAST_30_DAYS' as date_window_key, '30 ngày' as date_window_label, 30 as window_days, 3 as sort_order
),

visits_by_window as (
    select
        w.date_window_key,
        w.date_window_label,
        w.sort_order,
        v.visit_date,
        v.dot_dieu_tri_id
    from {{ ref('int_clinical__patient_visits_classified') }} v
    cross join date_window_options w
    where datediff(current_date, cast(v.visit_date as date)) between 0 and w.window_days - 1
),

daily_visits as (
    select
        date_window_key,
        date_window_label,
        sort_order,
        date_trunc('day', visit_date) as stat_date,
        count(dot_dieu_tri_id) as total_visits
    from visits_by_window
    group by 1, 2, 3, 4
),

revenue_by_window as (
    select
        w.date_window_key,
        w.date_window_label,
        w.sort_order,
        r.order_time,
        r.total_amount
    from {{ ref('int_finance__revenue_transactions') }} r
    cross join date_window_options w
    where datediff(current_date, cast(r.order_time as date)) between 0 and w.window_days - 1
),

daily_revenue as (
    select
        date_window_key,
        date_window_label,
        sort_order,
        date_trunc('day', order_time) as stat_date,
        sum(total_amount) as total_revenue
    from revenue_by_window
    group by 1, 2, 3, 4
),

joined_daily_data as (
    select
        coalesce(v.date_window_key, r.date_window_key) as date_window_key,
        coalesce(v.date_window_label, r.date_window_label) as date_window_label,
        coalesce(v.sort_order, r.sort_order) as sort_order,
        coalesce(v.stat_date, r.stat_date) as stat_date,
        coalesce(v.total_visits, 0) as total_visits,
        coalesce(r.total_revenue, 0) as total_revenue
    from daily_visits v
    full outer join daily_revenue r
        on v.date_window_key = r.date_window_key
       and v.stat_date = r.stat_date
),

final_formatting as (
    select
        date_window_key,
        date_window_label,
        sort_order,
        extract(year from stat_date) as stat_year,
        extract(month from stat_date) as stat_month,
        extract(day from stat_date) as stat_day,
        stat_date,
        'T' || lpad(cast(extract(month from stat_date) as string), 2, '0') as month_label,
        cast(stat_date as string) as day_label,
        total_visits,
        total_revenue / 1000000.0 as revenue_millions
    from joined_daily_data
)

select * from final_formatting