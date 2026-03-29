-- Extract service orders and execution times, filtering out soft-deleted records
SELECT 
    sub_services.service_order_id,
    sub_services.encounter_id,
    sub_services.service_id,
    sub_services.service_category_id,
    sub_services.order_date,
    sub_services.execution_date,
    sub_services.quantity
FROM (
    SELECT 
        id AS service_order_id,
        nb_dot_dieu_tri_id AS encounter_id,
        dich_vu_id AS service_id,
        loai_dich_vu AS service_category_id,
        CAST(SUBSTR(thoi_gian_chi_dinh, 1, 10) AS DATE) AS order_date,
        -- Add execution time to track dispensed status
        CAST(SUBSTR(thoi_gian_thuc_hien, 1, 10) AS DATE) AS execution_date,
        so_luong AS quantity,
        
    FROM {{ source('core_his', 'ct_dich_vu_iceberg') }}
    WHERE deleted = 0 
      AND active = true
) AS sub_services