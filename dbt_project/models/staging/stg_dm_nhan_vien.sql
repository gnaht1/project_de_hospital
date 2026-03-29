-- Extract active employees from catalog
SELECT 
    id AS employee_id,
    ten AS full_name,
    hoc_ham_hoc_vi_id,
    active,
    CAST(SUBSTR(created_at, 1, 10) AS DATE) AS created_date
FROM {{ source('core_his', 'dm_nhan_vien_iceberg') }}
WHERE deleted = 0