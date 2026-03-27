-- Create dimension table for departments
SELECT 
    sub_dim.department_key, 
    sub_dim.department_name
FROM (
    SELECT 
        department_key, 
        department_name 
    FROM {{ ref('stg_dm_khoa') }}
) AS sub_dim