with revenue_data as (
    select * from {{ ref('int_finance__paid_service_transactions') }}
),

categorized_revenue as (
    select
        extract(year from payment_time) as stat_year,
        extract(month from payment_time) as stat_month_num,
        extract(day from payment_time) as stat_day,
        cast(date_trunc('day', payment_time) as date) as stat_date,
        case
            when loai_dich_vu_id = 10 then 'Kham benh'
            when loai_dich_vu_id = 20 then 'Xet nghiem'
            when loai_dich_vu_id = 30 then 'CDHA'
            when loai_dich_vu_id in (40, 45) then 'PTTT'
            when loai_dich_vu_id = 90 then 'Thuoc'
            else coalesce(service_type_name, 'Khac')
        end as service_group,
        total_amount
    from revenue_data
    where payment_time is not null
),

daily_summary as (
    select
        stat_year,
        stat_month_num,
        stat_day,
        stat_date,
        service_group,
        sum(total_amount) as group_revenue,
        case
            when service_group = 'Kham benh' then 1
            when service_group = 'Xet nghiem' then 2
            when service_group = 'CDHA' then 3
            when service_group = 'Thuoc' then 4
            when service_group = 'PTTT' then 5
            else 99
        end as display_order
    from categorized_revenue
    group by
        stat_year,
        stat_month_num,
        stat_day,
        stat_date,
        service_group,
        case
            when service_group = 'Kham benh' then 1
            when service_group = 'Xet nghiem' then 2
            when service_group = 'CDHA' then 3
            when service_group = 'Thuoc' then 4
            when service_group = 'PTTT' then 5
            else 99
        end
),

percentage_calc as (
    select
        stat_year,
        stat_month_num,
        stat_day,
        stat_date,
        service_group,
        group_revenue,
        display_order,
        cast(group_revenue as double) / sum(group_revenue) over(partition by stat_date) as revenue_percentage
    from daily_summary
)

select *
from percentage_calc
