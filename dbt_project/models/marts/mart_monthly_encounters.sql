-- models/marts/mart_monthly_encounters.sql
-- Aggregate total encounters by month to calculate averages
SELECT 
    sub_monthly.admission_month,
    sub_monthly.total_encounters
FROM (
    SELECT 
        -- Extract Year-Month (YYYY-MM) from the admission date
        SUBSTR(CAST(admission_date AS STRING), 1, 7) AS admission_month,
        COUNT(encounter_id) AS total_encounters
    FROM {{ ref('fact_encounters') }}
    GROUP BY SUBSTR(CAST(admission_date AS STRING), 1, 7)
) AS sub_monthly