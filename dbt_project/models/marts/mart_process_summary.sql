-- models/marts/mart_process_summary.sql
-- Aggregate metrics from various process tables into a single unified catalog using UNION ALL
SELECT 
    sub_summary.process_name,
    sub_summary.total_count,
    sub_summary.status_tag
FROM (
    -- Subquery 1: Encounters (Tiếp Đón)
    SELECT 
        'Tiếp Đón & Khám Bệnh' AS process_name,
        COUNT(encounter_id) AS total_count,
        'Hoạt động' AS status_tag
    FROM {{ ref('fact_encounters') }}
    
    UNION ALL
    
    -- Subquery 2: Paraclinical Services (Cận Lâm Sàng)
    SELECT 
        'Cận Lâm Sàng' AS process_name,
        COUNT(service_order_id) AS total_count,
        'Hoạt động' AS status_tag
    FROM {{ ref('fact_service_orders') }}
    WHERE service_category_id IN (20, 30)
    
    UNION ALL
    
    -- Subquery 3: Prescriptions (Cấp Phát Thuốc)
    SELECT 
        'Cấp Phát Thuốc' AS process_name,
        COUNT(service_order_id) AS total_count,
        'Hoạt động' AS status_tag
    FROM {{ ref('fact_prescriptions') }}
    
    UNION ALL
    
    -- Subquery 4: Receipts (Thanh Toán & Thu Ngân)
    SELECT 
        'Thanh Toán & Thu Ngân' AS process_name,
        COUNT(receipt_id) AS total_count,
        'Hoạt động' AS status_tag
    FROM {{ ref('fact_receipts') }}
) AS sub_summary