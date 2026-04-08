SELECT
    id AS phieu_thu_id,
    thanh_tien,
    CAST(thoi_gian_thanh_toan AS TIMESTAMP) AS thoi_gian_thanh_toan,
    thanh_toan AS trang_thai_thanh_toan,
    active,
    deleted
FROM {{ source('raw_hospital', 'ct_phieu_thu_iceberg') }}