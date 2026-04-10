with stg_dich_vu as (
    select * from {{ ref('stg_hospital_core__ct_dich_vu') }}
    -- Filter for examination services only
    where loai_dich_vu_id = 10 
),

stg_nhan_vien as (
    select * from {{ ref('stg_hospital_core__dm_nhan_vien') }}
),

stg_chuyen_khoa as (
    select * from {{ ref('stg_hospital_core__dm_chuyen_khoa') }}
),

joined_data as (
    select
        dv.dich_vu_id,
        dv.order_time,
        
        -- Fallback mechanism: Use service specialty ID first. 
        -- If NULL, fallback to doctor's primary specialty ID.
        coalesce(dv.nb_chuyen_khoa_id, nv.primary_chuyen_khoa_id) as final_ck_id,
        
        coalesce(ck.specialty_code, 'UNKNOWN') as specialty_code,
        coalesce(ck.specialty_name, 'Chưa xác định') as specialty_name
        
    from stg_dich_vu dv
    -- Bridge 1: Join to doctor to get their primary specialty
    left join stg_nhan_vien nv 
        on dv.bac_si_chi_dinh_id = nv.nhan_vien_id
        
    -- Bridge 2: Join to specialty dimension using the fallback ID
    left join stg_chuyen_khoa ck 
        on coalesce(dv.nb_chuyen_khoa_id, nv.primary_chuyen_khoa_id) = ck.chuyen_khoa_id
)

select * from joined_data