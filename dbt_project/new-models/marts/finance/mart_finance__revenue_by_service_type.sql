with revenue_data as (
    select * from {{ ref('int_finance__revenue_transactions') }}
),

categorized_revenue as (
    select
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        date_trunc('month', cast(order_time as timestamp)) as stat_date,
        
        -- Gom nhóm ID theo bảng mapping bạn vừa cung cấp
        case 
            when loai_dich_vu_id = 10 then 'Khám bệnh'
            when loai_dich_vu_id = 20 then 'Xét nghiệm'
            when loai_dich_vu_id = 30 then 'CĐHA'
            when loai_dich_vu_id in (40, 45) then 'PTTT'
            when loai_dich_vu_id = 90 then 'Thuốc'
            else 'Khác'
        end as service_group,
        
        total_amount
    from revenue_data
),

monthly_summary as (
    -- Bước 1: Tính tổng tiền cho từng nhóm trong từng tháng
    select
        stat_year,
        stat_month,
        stat_date,
        'T' || lpad(cast(stat_month as string), 2, '0') as month_label,
        service_group,
        sum(total_amount) as group_revenue
    from categorized_revenue
    group by 1, 2, 3, 4, 5
),

percentage_calc as (
    -- Bước 2: Dùng Window Function để chia % (giống hệt chart Tái Khám)
    select
        stat_year,
        stat_month,
        stat_date,
        month_label,
        service_group,
        
        group_revenue,
        
        -- Tính tỷ trọng % của nhóm đó so với tổng doanh thu cả tháng
        cast(group_revenue as double) / sum(group_revenue) over(partition by stat_year, stat_month) as revenue_percentage
        
    from monthly_summary
)

select * from percentage_calc