with pharmacy_data as (
    select * from {{ ref('int_pharmacy__medication_transactions') }}
),

date_window_options as (
    select 'TODAY' as date_window_key, 'Hôm nay' as date_window_label, 1 as window_days, 1 as sort_order
    union all
    select 'LAST_7_DAYS' as date_window_key, '7 ngày' as date_window_label, 7 as window_days, 2 as sort_order
    union all
    select 'LAST_30_DAYS' as date_window_key, '30 ngày' as date_window_label, 30 as window_days, 3 as sort_order
),

pharmacy_data_by_window as (
    select
        w.date_window_key,
        w.date_window_label,
        w.sort_order,
        p.order_time,
        p.dot_dieu_tri_id,
        p.net_patient_amount
    from pharmacy_data p
    cross join date_window_options w
    where datediff(current_date, cast(p.order_time as date)) between 0 and w.window_days - 1
),

monthly_stats as (
    select
        date_window_key,
        date_window_label,
        sort_order,
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        date_trunc('month', cast(order_time as timestamp)) as stat_date,
        count(distinct dot_dieu_tri_id) as dispensed_prescriptions
    from pharmacy_data_by_window
    where net_patient_amount > 0
    group by 1, 2, 3, 4, 5, 6
)

select * from monthly_stats