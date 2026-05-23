with dot_dieu_tri as (
    select * from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
    -- Đã bỏ bộ lọc ngày ở đây để lưu lịch sử All-time
),

benh_nhan as (
    select * from {{ ref('stg_hospital_core__dm_benh_nhan') }}
),

doi_tuong as (
    select * from {{ ref('stg_hospital_core__dm_doi_tuong_kcb') }}
),

khoa as (
    select khoa_id, department_name from {{ ref('stg_hospital_core__dm_khoa') }}
)

select
    dt.dot_dieu_tri_id as nb_dot_dieu_tri_id,
    dt.admission_time,
    
    bn.patient_code as ma_hs,
    bn.patient_name as benh_nhan,
    kp.department_name as khoa,
    
    case 
        when dt.is_health_check = true then 'KSK Đoàn'
        else coalesce(dt_kcb.patient_type_name, 'Dịch vụ')
    end as doi_tuong
    
from dot_dieu_tri dt
left join benh_nhan bn on dt.nb_thong_tin_id = bn.patient_id
left join doi_tuong dt_kcb on dt.doi_tuong_kcb_id = dt_kcb.doi_tuong_kcb_id
left join khoa kp on dt.khoa_id = kp.khoa_id