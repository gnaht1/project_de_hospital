with dv as (

    select * 
    from {{ ref('stg_hospital_core__dm_dich_vu') }}

),

nhom as (

    select * 
    from {{ ref('stg_hospital_core__dm_nhom_dich_vu_cap1') }}

),

joined as (

    select
        dv.dich_vu_id,
        dv.ten_dich_vu,
        dv.loai_dich_vu_id,

        dv.nhom_dich_vu_cap1_id,
        nhom.ten_nhom_dich_vu_cap1 as nhom_dich_vu_cap1

    from dv
    left join nhom
        on dv.nhom_dich_vu_cap1_id = nhom.nhom_dich_vu_cap1_id

)

select * from joined