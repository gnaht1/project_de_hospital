-- models/marts/mart_peak_month_encounters.sql
-- Find the month with the highest number of encounters and format the label
SELECT 
    sub_peak.peak_month_label,
    sub_peak.total_encounters
FROM (
    SELECT 
        -- Extract month from date and format it as 'T7', 'T8', etc.
        CONCAT('T', CAST(MONTH(admission_date) AS STRING)) AS peak_month_label,
        COUNT(encounter_id) AS total_encounters
    FROM {{ ref('fact_encounters') }}
    GROUP BY MONTH(admission_date)
    -- Sort descending and keep only the top 1 row
    ORDER BY total_encounters DESC
    LIMIT 1
) AS sub_peak