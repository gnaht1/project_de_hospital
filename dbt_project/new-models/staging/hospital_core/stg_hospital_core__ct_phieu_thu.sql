SELECT
    id AS phieu_thu_id,
    nb_dot_dieu_tri_id,
    thanh_tien,
    CAST(thoi_gian_thanh_toan AS TIMESTAMP) AS thoi_gian_thanh_toan,
    thanh_toan AS trang_thai_thanh_toan,
    CAST(thoi_gian_tao_phieu AS TIMESTAMP) AS thoi_gian_tao_phieu,
    CAST(thu_ngan_id AS INT) AS thu_ngan_id,
    COALESCE(tien_giam_gia, 0) AS tien_giam_gia,
    COALESCE(tien_mien_giam_dich_vu, 0) AS tien_mien_giam_dich_vu,
    COALESCE(tien_mien_giam_phieu_thu, 0) AS tien_mien_giam_phieu_thu,
    COALESCE(tien_mien_giam_phieu_thu_nhap_vao, 0) AS tien_mien_giam_phieu_thu_nhap_vao,
    COALESCE(tien_hoan_tra, 0) AS tien_hoan_tra,
    COALESCE(phan_tram_mien_giam, 0) AS phan_tram_mien_giam,
    loai_phieu_thu,
    loai_mien_giam,
    active,
    deleted
FROM {{ source('raw_hospital', 'ct_phieu_thu_iceberg') }}
