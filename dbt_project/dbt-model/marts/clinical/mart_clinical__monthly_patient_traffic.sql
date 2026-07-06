with patient_visits as (
    -- Tái sử dụng bảng Silver từ Dashboard trước
    select * from {{ ref('int_clinical__patient_visits_classified') }}
),

monthly_traffic as (
    select
        extract(year from created_at) as stat_year,
        extract(month from created_at) as stat_month,
        
        -- Mốc thời gian để Superset so sánh (Time Range)
        date_trunc('month', cast(created_at as timestamp)) as stat_date,
        
        -- 1. Tổng số LƯỢT khám (Mỗi lần đăng ký là 1 lượt)
        count(dot_dieu_tri_id) as total_visits,
        
        -- 2. Tổng số BỆNH NHÂN (1 người đến n lần trong tháng chỉ tính là 1)
        count(distinct nb_thong_tin_id) as unique_patients
        
    from patient_visits
    where created_at is not null
    group by 1, 2, 3
)

select * from monthly_traffic
