-- Create monthly snapshot of active staff and doctors using a subquery
SELECT 
    m.report_month,
    COUNT(e.employee_id) AS tong_nv,
    SUM(e.is_doctor) AS bac_si
FROM (
    -- Get distinct months from encounters as a timeline spine
    SELECT DISTINCT DATE_TRUNC('month', admission_date) AS report_month
    FROM {{ ref('fact_encounters') }}
    WHERE admission_date IS NOT NULL
) AS m
LEFT JOIN {{ ref('dim_employees') }} e
    -- Logic: Employee was created on or before the report month AND is currently active
    ON DATE_TRUNC('month', e.created_date) <= m.report_month
    AND e.active = true
GROUP BY 
    m.report_month