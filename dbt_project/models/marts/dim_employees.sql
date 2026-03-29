-- Create employee dimension with doctor flag using degree code
SELECT 
    e.employee_id,
    e.full_name,
    e.active,
    e.created_date,
    CASE 
        WHEN h.degree_code LIKE '%DR%' THEN 1 
        ELSE 0 
    END AS is_doctor
FROM {{ ref('stg_dm_nhan_vien') }} e
LEFT JOIN {{ ref('stg_dm_hoc_ham_hoc_vi') }} h
    ON e.hoc_ham_hoc_vi_id = h.degree_id