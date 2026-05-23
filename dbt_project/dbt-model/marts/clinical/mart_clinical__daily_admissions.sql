with treatment_episodes as (
    -- Lấy dữ liệu đợt điều trị từ lớp Staging
    select * from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
),

daily_admissions as (
    select
        date_trunc('day', admission_time) as stat_date,
        
        -- Tổng số đợt điều trị (1 người có thể có 2 đợt/ngày nếu sáng khám nội, chiều cấp cứu)
        count(dot_dieu_tri_id) as total_episodes,
        
        -- Nếu muốn đếm số CON NGƯỜI đến khám hôm nay thay vì số LƯỢT:
        count(distinct dot_dieu_tri_id) as unique_patients,

        count(case when is_health_check = true then dot_dieu_tri_id end) as total_ksk_episodes
        
    from treatment_episodes
    group by 1
)

select * from daily_admissions