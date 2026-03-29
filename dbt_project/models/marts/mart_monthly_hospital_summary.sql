-- Create comprehensive monthly summary for hospital metrics using subqueries
SELECT
    spine.report_month,
    COALESCE(rev.doanh_thu_trieu, 0) AS doanh_thu_trieu,
    COALESCE(enc.luot_kham, 0) AS luot_kham,
    COALESCE(cls.dich_vu_cls, 0) AS dich_vu_cls,
    COALESCE(rx.toa_thuoc, 0) AS toa_thuoc,
    COALESCE(pat.bn_moi, 0) AS bn_moi,
    COALESCE(ksk.hd_ksk, 0) AS hd_ksk
FROM (
    -- Time spine based on distinct encounter months
    SELECT DISTINCT DATE_TRUNC('month', admission_date) AS report_month
    FROM {{ ref('fact_encounters') }}
    WHERE admission_date IS NOT NULL
) AS spine
LEFT JOIN (
    -- Metric 1: Revenue (Doanh Thu) converted to Millions
    SELECT 
        DATE_TRUNC('month', payment_date) AS month_key,
        SUM(total_amount) / 1000000.0 AS doanh_thu_trieu
    FROM {{ ref('fact_receipts') }}
    GROUP BY DATE_TRUNC('month', payment_date)
) AS rev ON spine.report_month = rev.month_key
LEFT JOIN (
    -- Metric 2: Encounters (Lượt Khám)
    SELECT 
        DATE_TRUNC('month', admission_date) AS month_key,
        COUNT(encounter_id) AS luot_kham
    FROM {{ ref('fact_encounters') }}
    GROUP BY DATE_TRUNC('month', admission_date)
) AS enc ON spine.report_month = enc.month_key
LEFT JOIN (
    -- Metric 3: Paraclinical Services (Dịch Vụ CLS: X-Quang, Xét nghiệm...)
    SELECT 
        DATE_TRUNC('month', f.order_date) AS month_key,
        COUNT(f.service_order_id) AS dich_vu_cls
    FROM {{ ref('fact_service_orders') }} f
    WHERE f.service_category_id IN (20, 30) -- Assuming 20=Xét nghiệm, 30=CĐHA
    GROUP BY DATE_TRUNC('month', f.order_date)
) AS cls ON spine.report_month = cls.month_key
LEFT JOIN (
    -- Metric 4: Prescriptions (Toa Thuốc - count distinct encounters with medicines)
    SELECT 
        DATE_TRUNC('month', execution_date) AS month_key,
        COUNT(DISTINCT encounter_id) AS toa_thuoc
    FROM {{ ref('fact_prescriptions') }}
    GROUP BY DATE_TRUNC('month', execution_date)
) AS rx ON spine.report_month = rx.month_key
LEFT JOIN (
    -- Metric 5: New Patients (BN Mới - count by creation date)
    SELECT 
        DATE_TRUNC('month', CAST(SUBSTR(created_at, 1, 10) AS DATE)) AS month_key,
        COUNT(patient_key) AS bn_moi
    FROM {{ ref('stg_dm_benh_nhan') }}
    GROUP BY DATE_TRUNC('month', CAST(SUBSTR(created_at, 1, 10) AS DATE))
) AS pat ON spine.report_month = pat.month_key
LEFT JOIN (
    -- Metric 6: Health Checkup Contracts (HĐ KSK)
    SELECT 
        DATE_TRUNC('month', created_date) AS month_key,
        COUNT(contract_id) AS hd_ksk
    FROM {{ ref('stg_dm_hop_dong_ksk') }}
    GROUP BY DATE_TRUNC('month', created_date)
) AS ksk ON spine.report_month = ksk.month_key

ORDER BY spine.report_month