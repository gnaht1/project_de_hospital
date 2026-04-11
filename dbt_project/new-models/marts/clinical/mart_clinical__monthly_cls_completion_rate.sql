with cls_data as (
    select * from {{ ref('int_clinical__cls_by_room') }}
),

monthly_stats as (
    -- Chỉ cần gom nhóm theo tháng và đếm số lượng
    select
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        
        -- Cột stat_date này Superset sẽ dùng làm trục thời gian (Time Column)
        date_trunc('month', cast(order_time as timestamp)) as stat_date,
        
        -- Mẫu số: Tổng dịch vụ
        count(dich_vu_id) as total_cls_orders,
        
        -- Tử số: Chỉ đếm các dịch vụ đã Hoàn thành
        sum(case when execution_status = 'Hoàn thành' then 1 else 0 end) as completed_cls_orders
        
    from cls_data
    group by 1, 2, 3
),

rate_calculation as (
    select
        stat_year,
        stat_month,
        stat_date,
        'T' || lpad(cast(stat_month as string), 2, '0') as month_label,
        total_cls_orders,
        completed_cls_orders,
        
        -- Chỉ tính tỷ lệ của tháng đó là xong (Không cần LAG nữa)
        case 
            when total_cls_orders > 0 then cast(completed_cls_orders as double) / total_cls_orders 
            else 0 
        end as completion_rate,
        -- Mục tiêu cố định 95%
        0.95 as target_rate
        
    from monthly_stats
)

select * from rate_calculation