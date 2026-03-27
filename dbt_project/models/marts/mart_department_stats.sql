-- Aggregate all key metrics (patients, pending services, revenue) by department
SELECT
    d.department_name,
    COALESCE(e.active_patients, 0) AS active_patients,
    COALESCE(cls.pending_cls_orders, 0) AS pending_cls_orders,
    COALESCE(e.completed_patients, 0) AS completed_patients,
    COALESCE(r.total_revenue, 0) AS total_revenue,
    -- Determine department status based on active patient load threshold (e.g., > 30 is Busy)
    CASE
        WHEN COALESCE(e.active_patients, 0) >= 30 THEN 'Bận'
        ELSE 'Bình thường'
    END AS department_status
FROM (
    SELECT department_key, department_name 
    FROM {{ ref('dim_departments') }}
) AS d

-- Subquery 1: Active and Completed Encounters
LEFT JOIN (
    SELECT
        khoa_id AS department_key,
        SUM(CASE WHEN thoi_gian_ra_vien IS NULL THEN 1 ELSE 0 END) AS active_patients,
        SUM(CASE WHEN thoi_gian_ra_vien IS NOT NULL THEN 1 ELSE 0 END) AS completed_patients
    FROM {{ source('core_his', 'ct_dot_dieu_tri_iceberg') }}
    WHERE deleted = 0
    GROUP BY khoa_id
) AS e ON d.department_key = e.department_key

-- Subquery 2: Pending Paraclinical Orders (CLS Chờ)
LEFT JOIN (
    SELECT
        khoa_chi_dinh_id AS department_key,
        COUNT(id) AS pending_cls_orders
    FROM {{ source('core_his', 'ct_dich_vu_iceberg') }}
    -- Filter for Lab (20) and Imaging (30) that are not yet executed
    WHERE deleted = 0 
      AND loai_dich_vu IN (20, 30) 
      AND thoi_gian_thuc_hien IS NULL
    GROUP BY khoa_chi_dinh_id
) AS cls ON d.department_key = cls.department_key

-- Subquery 3: Revenue from Receipts linked to Department
LEFT JOIN (
    SELECT
        sub_enc.khoa_id AS department_key,
        SUM(sub_rec.thanh_tien) AS total_revenue
    FROM (
        SELECT nb_dot_dieu_tri_id, thanh_tien
        FROM {{ source('core_his', 'ct_phieu_thu_iceberg') }}
        WHERE deleted = 0 AND thanh_toan = 50
    ) AS sub_rec
    INNER JOIN (
        SELECT id, khoa_id
        FROM {{ source('core_his', 'ct_dot_dieu_tri_iceberg') }}
        WHERE deleted = 0
    ) AS sub_enc ON sub_rec.nb_dot_dieu_tri_id = sub_enc.id
    GROUP BY sub_enc.khoa_id
) AS r ON d.department_key = r.department_key