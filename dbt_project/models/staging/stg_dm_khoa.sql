-- Extract active departments from catalog
SELECT 
    sub_dept.department_key, 
    sub_dept.department_name
FROM (
    SELECT 
        id AS department_key, 
        ten AS department_name
    FROM {{ source('core_his', 'dm_khoa_iceberg') }}
    WHERE deleted = 0 AND active = true
) AS sub_dept