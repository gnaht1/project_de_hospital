with dot_dieu_tri as (
    select * from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
    -- Temporary turn off  filter for testing
    where date_trunc('day', admission_time) = '2026-02-20'
),

benh_nhan as (
    -- Fetch patient demographics (Name, Gender, Code)
    select * from {{ ref('stg_hospital_core__dm_benh_nhan') }}
),

doi_tuong as (
    -- Fetch patient category names (e.g., BHYT, Viện phí)
    select * from {{ ref('stg_hospital_core__dm_doi_tuong_kcb') }}
),

khoa as (
    -- Fetch department names
    select khoa_id, department_name from {{ ref('stg_hospital_core__dm_khoa') }}
),

joined_list as (
    select
        bn.patient_code as ma_hs,
        bn.patient_name as benh_nhan,
        kp.department_name as khoa,
        
        -- Priority for patient category
        case 
            when dt.is_health_check = true then 'KSK Đoàn'
            else coalesce(dt_kcb.patient_type_name, 'Dịch vụ')
        end as doi_tuong,
        
        date_format(admission_time, 'HH:mm') as thoi_gian_vao
        
    from dot_dieu_tri dt
    left join benh_nhan bn on dt.nb_thong_tin_id = bn.patient_id
    left join doi_tuong dt_kcb on dt.doi_tuong_kcb_id = dt_kcb.doi_tuong_kcb_id
    left join khoa kp on dt.khoa_id = kp.khoa_id
)

select * from joined_list
-- Sort descending so the most recent patient appears at the top
order by thoi_gian_vao desc