-- Aggregate monthly paraclinical services (CLS) for charting
SELECT 
    DATE_TRUNC('month', f.order_date) AS order_month,
    d.cls_group,
    COUNT(f.service_order_id) AS total_services
FROM {{ ref('fact_service_orders') }} f
LEFT JOIN {{ ref('dim_service_categories') }} d
    ON f.service_category_id = d.service_category_id
WHERE f.order_date IS NOT NULL
GROUP BY 
    DATE_TRUNC('month', f.order_date), 
    d.cls_group