with source as (
    select * from {{ source('raw_hospital', 'ct_nguon_nb_iceberg') }}
),

renamed_and_casted as (
    select
        nb_dot_dieu_tri_id,
        ghi_chu,
        cast(nguoi_gioi_thieu_id as int) as nguoi_gioi_thieu_id,
        cast(nguon_nb_id as int) as nguon_nb_id,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from source
    where nb_dot_dieu_tri_id is not null
)

select * from renamed_and_casted
