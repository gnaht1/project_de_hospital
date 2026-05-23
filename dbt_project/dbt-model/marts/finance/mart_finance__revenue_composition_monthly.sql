with revenue_data as (
    -- Lấy cục tiền sạch từ lớp Silver đã làm ở các bước trước
    select * from {{ ref('int_finance__revenue_transactions') }}
),

visit_data as (
    -- Lấy thông tin đợt điều trị từ lớp Staging để biết ai là khách KSK
    select dot_dieu_tri_id, is_health_check 
    from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
),

joined_data as (
    select
        extract(year from r.order_time) as stat_year,
        extract(month from r.order_time) as stat_month,
        date_trunc('month', cast(r.order_time as timestamp)) as stat_date,
        
        -- Phân loại luồng tiền: KSK vs Dịch vụ thường
        case 
            when v.is_health_check = true then 'KSK'
            else 'Dịch vụ'
        end as revenue_type,
        
        r.total_amount
        
    from revenue_data r
    left join visit_data v on r.dot_dieu_tri_id = v.dot_dieu_tri_id
),

monthly_summary as (
    -- Gom nhóm tính tổng doanh thu theo từng loại trong tháng
    select
        stat_year,
        stat_month,
        stat_date,
        'T' || lpad(cast(stat_month as string), 2, '0') as month_label,
        revenue_type,
        sum(total_amount) as type_revenue
    from joined_data
    group by 1, 2, 3, 4, 5
),

percentage_calc as (
    -- Tính phần trăm tỷ trọng (Window Function)
    select
        *,
        -- Tổng doanh thu của loại đó chia cho Tổng doanh thu cả tháng
        cast(type_revenue as double) / sum(type_revenue) over(partition by stat_year, stat_month) as revenue_percentage
    from monthly_summary
)

select * from percentage_calc