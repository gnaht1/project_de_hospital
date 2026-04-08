-- Create monthly revenue mart by department using subquery for base joins
SELECT 
    DATE_TRUNC('month', base_data.payment_date) AS payment_month,
    base_data.department_name,
    SUM(base_data.total_amount) / 1000000.0 AS doanh_thu_trieu_vnd
FROM (
    -- Join receipts with encounters and departments to get revenue context
    SELECT 
        r.payment_date,
        r.total_amount,
        d.department_name
    FROM {{ ref('fact_receipts') }} r
    LEFT JOIN {{ ref('fact_encounters') }} e
        ON r.encounter_id = e.encounter_id
    LEFT JOIN {{ ref('dim_departments') }} d
        ON e.department_key = d.department_key
    WHERE r.payment_date IS NOT NULL
      AND d.department_name IS NOT NULL
) AS base_data
GROUP BY 
    DATE_TRUNC('month', base_data.payment_date),
    base_data.department_name