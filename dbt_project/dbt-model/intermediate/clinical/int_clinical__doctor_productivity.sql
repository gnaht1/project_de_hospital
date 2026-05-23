with stg_dich_vu as (
    select * from {{ ref('stg_hospital_core__ct_dich_vu') }}
    -- Filter for examination services only
    where loai_dich_vu_id = 10 
),

stg_nhan_vien as (
    select * from {{ ref('stg_hospital_core__dm_nhan_vien') }}
),

joined_data as (
    select
        dv.dich_vu_id,
        dv.order_time,
        
        -- Get doctor ID and Name
        dv.bac_si_chi_dinh_id as doctor_id,
        coalesce(nv.doctor_name, 'Unknown Doctor') as doctor_name
        
    from stg_dich_vu dv
    -- Join to employee dimension to get the doctor's name
    left join stg_nhan_vien nv 
        on dv.bac_si_chi_dinh_id = nv.nhan_vien_id
)

select * from joined_data