-- models/marts/mart_daily_hospital_metrics.sql
-- Create a daily aggregated mart for dual-axis charts, combining encounters and revenue
SELECT
    sub_dates.report_date,
    COALESCE(sub_encounters.total_encounters, 0) AS total_encounters,
    -- Divide by 1,000,000 to pre-calculate revenue in millions (Tr) for the chart
    COALESCE(sub_receipts.total_revenue, 0) / 1000000.0 AS total_revenue_millions
FROM (
    -- Get a distinct list of dates from both encounters and receipts to handle days with only one type of activity
    SELECT admission_date AS report_date FROM {{ ref('fact_encounters') }}
    UNION
    SELECT payment_date AS report_date FROM {{ ref('fact_receipts') }}
) AS sub_dates

LEFT JOIN (
    -- Aggregate daily encounters
    SELECT
        admission_date,
        COUNT(DISTINCT encounter_id) AS total_encounters
    FROM {{ ref('fact_encounters') }}
    GROUP BY admission_date
) AS sub_encounters 
ON sub_dates.report_date = sub_encounters.admission_date

LEFT JOIN (
    -- Aggregate daily revenue
    SELECT
        payment_date,
        SUM(total_amount) AS total_revenue
    FROM {{ ref('fact_receipts') }}
    GROUP BY payment_date
) AS sub_receipts 
ON sub_dates.report_date = sub_receipts.payment_date