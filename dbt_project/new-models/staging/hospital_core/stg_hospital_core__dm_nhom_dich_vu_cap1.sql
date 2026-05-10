WITH source AS (
    SELECT * FROM {{ source('raw_hospital', 'dm_nhom_dich_vu_cap1_iceberg') }}
)

SELECT 
    id AS nhom_dich_vu_cap1_id,
    ten AS ten_nhom_dich_vu_cap1,
    loai_dich_vu AS loai_dich_vu_id
FROM source