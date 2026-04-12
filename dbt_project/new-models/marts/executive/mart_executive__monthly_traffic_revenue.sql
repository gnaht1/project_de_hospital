with monthly_visits as (
    -- Gom nhóm lượt khám theo tháng
    select
        date_trunc('month', visit_date) as stat_date,
        count(dot_dieu_tri_id) as total_visits
    from {{ ref('int_clinical__patient_visits_classified') }}
    group by 1
),

monthly_revenue as (
    -- Gom nhóm doanh thu theo tháng
    select
        date_trunc('month', order_time) as stat_date,
        sum(total_amount) as total_revenue
    from {{ ref('int_finance__revenue_transactions') }}
    group by 1
),

joined_monthly_data as (
    -- Gộp 2 bảng lại (FULL OUTER JOIN để phòng trường hợp có tháng có khám nhưng không có doanh thu và ngược lại)
    select
        coalesce(v.stat_date, r.stat_date) as stat_date,
        coalesce(v.total_visits, 0) as total_visits,
        coalesce(r.total_revenue, 0) as total_revenue
    from monthly_visits v
    full outer join monthly_revenue r on v.stat_date = r.stat_date
),

final_formatting as (
    select
        extract(year from stat_date) as stat_year,
        extract(month from stat_date) as stat_month,
        stat_date,
        
        -- Tạo nhãn T01, T02... để Superset sort cho chuẩn
        'T' || lpad(cast(extract(month from stat_date) as string), 2, '0') as month_label,
        
        total_visits,
        
        -- Chia sẵn 1 triệu để hiện số nhỏ gọn trên chart (Triệu VNĐ)
        total_revenue / 1000000.0 as revenue_millions

    from joined_monthly_data
)

select * from final_formatting