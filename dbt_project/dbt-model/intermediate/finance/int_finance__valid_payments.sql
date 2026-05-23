SELECT
    phieu_thu_id,
    thanh_tien,
    thoi_gian_thanh_toan,
    YEAR(thoi_gian_thanh_toan) AS nam_doanh_thu,
    -- Extract the month as an integer
    MONTH(thoi_gian_thanh_toan) AS thang_doanh_thu,
    -- Format as YYYY-MM for optimal Superset time-series plotting
    DATE_FORMAT(thoi_gian_thanh_toan, 'yyyy-MM') AS ky_doanh_thu
FROM {{ ref('stg_hospital_core__ct_phieu_thu') }}
WHERE 
    active = true 
    AND deleted = 0 
    AND trang_thai_thanh_toan = 50 
    AND thoi_gian_thanh_toan IS NOT NULL