with stg_dot_dieu_tri as (
    select * from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
),

patient_first_visit as (
    -- Dùng nb_thong_tin_id thay vì ma_nb để xác định MỘT con người duy nhất
    select
        nb_thong_tin_id,
        min(cast(admission_time as date)) as first_visit_date
    from stg_dot_dieu_tri
    where nb_thong_tin_id is not null -- Đề phòng data lỗi
    group by nb_thong_tin_id
),

classified_visits as (
    select
        d.dot_dieu_tri_id,
        d.nb_thong_tin_id,
        d.admission_time,
        d.created_at,
        cast(d.admission_time as date) as visit_date,
        p.first_visit_date,
        
        -- Nếu ngày khám = ngày đầu tiên -> Bệnh nhân mới (1)
        case when cast(d.admission_time as date) = p.first_visit_date then 1 else 0 end as is_new_patient,
        
        -- Nếu ngày khám > ngày đầu tiên -> Bệnh nhân cũ/Tái khám (1)
        case when cast(d.admission_time as date) > p.first_visit_date then 1 else 0 end as is_return_patient
        
    from stg_dot_dieu_tri d
    left join patient_first_visit p on d.nb_thong_tin_id = p.nb_thong_tin_id
)

select * from classified_visits
