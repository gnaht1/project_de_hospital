with cls_data as (
    select * from {{ ref('int_clinical__cls_by_room') }}
),

daily_stats as (
    select
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        extract(day from order_time) as stat_day,
        cast(date_trunc('day', cast(order_time as timestamp)) as date) as stat_date,
        count(dich_vu_id) as total_cls_orders,
        sum(case when execution_status like 'Ho%' then 1 else 0 end) as completed_cls_orders
    from cls_data
    where order_time is not null
    group by 1, 2, 3, 4
),

rate_calculation as (
    select
        stat_year,
        stat_month,
        stat_day,
        stat_date,
        total_cls_orders,
        completed_cls_orders,
        case
            when total_cls_orders > 0 then cast(completed_cls_orders as double) / total_cls_orders
            else 0
        end as completion_rate,
        0.95 as target_rate
    from daily_stats
)

select *
from rate_calculation
order by stat_date
