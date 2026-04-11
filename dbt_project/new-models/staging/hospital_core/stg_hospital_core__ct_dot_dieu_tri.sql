with source as (
    -- Read from the raw table
    select * from {{ source('raw_hospital', 'ct_dot_dieu_tri_iceberg') }}
),

renamed_and_casted as (
    select
        -- Primary key
        id as dot_dieu_tri_id,
        -- Foreign key for patient classification
        doi_tuong_kcb as doi_tuong_kcb_id,
        ma_nb,
        -- Flags for classification
        cast(cap_cuu as boolean) as is_emergency,
        cast(kham_suc_khoe as boolean) as is_health_check,
        -- Date parsing
        cast(thoi_gian_vao_vien as timestamp) as admission_time,
        khoa_id,
        nb_thong_tin_id,
        -- Audit fields
        active as is_active,
        deleted as is_deleted

    from source
    -- Filter out soft-deleted records
    where deleted = 0 
      and active = true
)

select * from renamed_and_casted