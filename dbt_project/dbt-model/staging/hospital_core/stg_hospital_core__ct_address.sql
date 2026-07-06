with source as (
    select * from {{ source('raw_hospital', 'ct_address_iceberg') }}
),

renamed_and_casted as (
    select
        cast(nb_dot_dieu_tri_id as int) as nb_dot_dieu_tri_id,
        so_nha,
        so_nha_tam_tru,
        cast(xa_phuong_id as int) as xa_phuong_id,
        cast(nullif(xa_phuong_tam_tru_id, '') as int) as xa_phuong_tam_tru_id,
        cast(quan_huyen_id as int) as quan_huyen_id,
        cast(nullif(quan_huyen_tam_tru_id, '') as int) as quan_huyen_tam_tru_id,
        cast(tinh_thanh_pho_id as int) as tinh_thanh_pho_id,
        cast(nullif(tinh_thanh_pho_tam_tru_id, '') as int) as tinh_thanh_pho_tam_tru_id,
        dia_chi_cong_ty,
        ten_cong_ty,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
    where nb_dot_dieu_tri_id is not null
)

select * from renamed_and_casted
