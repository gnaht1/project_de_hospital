with stg_dich_vu as (

    select * 
    from {{ ref('stg_hospital_core__ct_dich_vu') }}

),

stg_loai_dich_vu as (

    select * 
    from {{ ref('stg_hospital_core__dm_loai_dich_vu') }}

),

stg_dm_dich_vu as (

    select * 
    from {{ ref('stg_hospital_core__dm_dich_vu') }}

),

stg_nhom_dich_vu as (

    select * 
    from {{ ref('stg_hospital_core__dm_nhom_dich_vu_cap1') }}

),

classified_services as (

    select
        dv.dich_vu_id,
        dv.order_time,

        extract(month from dv.order_time) as order_month_num,
        extract(year from dv.order_time) as order_year,

        -- LOGIC CŨ (GIỮ NGUYÊN)
        case 
            when ldv.loai_dich_vu_id = 20 then 'Xét Nghiệm'
            when ldv.loai_dich_vu_id = 30 then 'CĐHA'
            else 'Khác' 
        end as service_group,

        --  NEW: join ra nhóm dịch vụ cấp 1
        ndv.ten_nhom_dich_vu_cap1 as nhom_dich_vu_cap1

    from stg_dich_vu dv

    left join stg_loai_dich_vu ldv
        on dv.loai_dich_vu_id = ldv.loai_dich_vu_id

    -- join dịch vụ → lấy nhom_id
    left join stg_dm_dich_vu dmdv
        on dv.dm_dich_vu_id = dmdv.dich_vu_id

    -- join nhóm → lấy tên nhóm
    left join stg_nhom_dich_vu ndv
        on dmdv.nhom_dich_vu_cap1_id = ndv.nhom_dich_vu_cap1_id

    where dv.order_time is not null

)

select * from classified_services