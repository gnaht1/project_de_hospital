with cls_data as (
    select * from {{ ref('int_clinical__cls_by_room') }}
),

date_window_options as (
    select 'TODAY' as date_window_key, 'Hôm nay' as date_window_label, 1 as window_days, 1 as sort_order
    union all
    select 'LAST_7_DAYS' as date_window_key, '7 ngày' as date_window_label, 7 as window_days, 2 as sort_order
    union all
    select 'LAST_30_DAYS' as date_window_key, '30 ngày' as date_window_label, 30 as window_days, 3 as sort_order
),

cls_data_by_window as (
    select
        w.date_window_key,
        w.date_window_label,
        w.sort_order,
        c.order_time,
        c.dich_vu_id,
        c.execution_status
    from cls_data c
    cross join date_window_options w
    where datediff(current_date, cast(c.order_time as date)) between 0 and w.window_days - 1
),

monthly_stats as (
    select
        date_window_key,
        date_window_label,
        sort_order,
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        date_trunc('month', cast(order_time as timestamp)) as stat_date,
        count(dich_vu_id) as total_cls_orders,
        sum(case when execution_status like 'Ho%' then 1 else 0 end) as completed_cls_orders
    from cls_data_by_window
    group by 1, 2, 3, 4, 5, 6
),

rate_calculation as (
    select
        date_window_key,
        date_window_label,
        sort_order,
        stat_year,
        stat_month,
        stat_date,
        'T' || lpad(cast(stat_month as string), 2, '0') as month_label,
        total_cls_orders,
        completed_cls_orders,
        case
            when total_cls_orders > 0 then cast(completed_cls_orders as double) / total_cls_orders
            else 0
        end as completion_rate,
        0.95 as target_rate
    from monthly_stats
)

select * from rate_calculation
