with revenue_data as (
    select * from {{ ref('int_finance__paid_service_transactions') }}
),

categorized_revenue as (
    select
        extract(year from payment_time) as stat_year,
        extract(month from payment_time) as stat_month,
        cast(date_trunc('month', payment_time) as date) as stat_date,
        case
            when loai_dich_vu_id = 10 then 'Khám bệnh'
            when loai_dich_vu_id = 20 then 'Xét nghiệm'
            when loai_dich_vu_id = 30 then 'CĐHA'
            when loai_dich_vu_id in (40, 45) then 'PTTT'
            when loai_dich_vu_id = 90 then 'Thuốc'
            else coalesce(service_type_name, 'Khác')
        end as service_group,
        total_amount
    from revenue_data
),

monthly_summary as (
    select
        stat_year,
        stat_month,
        stat_date,
        'T' || lpad(cast(stat_month as string), 2, '0') as month_label,
        service_group,
        sum(total_amount) as group_revenue,
        case
            when service_group = 'Khám bệnh' then 1
            when service_group = 'Xét nghiệm' then 2
            when service_group = 'CĐHA' then 3
            when service_group = 'Thuốc' then 4
            when service_group = 'PTTT' then 5
            else 99
        end as display_order
    from categorized_revenue
    group by 1, 2, 3, 4, 5, 7
),

percentage_calc as (
    select
        stat_year,
        stat_month,
        stat_date,
        month_label,
        service_group,
        group_revenue,
        display_order,
        cast(group_revenue as double) / sum(group_revenue) over(partition by stat_year, stat_month) as revenue_percentage
    from monthly_summary
)

select * from percentage_calc
