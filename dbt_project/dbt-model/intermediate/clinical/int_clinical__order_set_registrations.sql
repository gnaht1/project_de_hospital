with order_sets as (
    select * from {{ ref('stg_hospital_core__ct_bo_chi_dinh') }}
),

order_set_dim as (
    select * from {{ ref('stg_hospital_core__dm_bo_chi_dinh') }}
),

visits as (
    select * from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
),

registrations as (
    select
        os.nb_bo_chi_dinh_id,
        os.nb_dot_dieu_tri_id,
        v.nb_thong_tin_id,
        os.bo_chi_dinh_id,
        coalesce(d.code_bo_chi_dinh, cast(os.bo_chi_dinh_id as string)) as code_bo_chi_dinh,
        d.ten_bo_chi_dinh,
        d.hop_dong_id,
        os.order_set_time,
        coalesce(os.order_set_time, v.admission_time, os.created_at) as event_time,
        v.admission_time,
        os.created_at,
        os.updated_at
    from order_sets os
    left join order_set_dim d
        on os.bo_chi_dinh_id = d.bo_chi_dinh_id
    left join visits v
        on os.nb_dot_dieu_tri_id = v.dot_dieu_tri_id
)

select * from registrations
