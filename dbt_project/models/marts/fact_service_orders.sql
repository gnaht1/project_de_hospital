-- models/marts/fact_service_orders.sql
-- Create service orders fact table for tracking clinical and paraclinical (CLS) services
SELECT 
    sub_fact.service_order_id,
    sub_fact.encounter_id,
    sub_fact.service_id,
    sub_fact.service_category_id,
    sub_fact.order_date,
    sub_fact.quantity
FROM (
    SELECT 
        service_order_id,
        encounter_id,
        service_id,
        service_category_id,
        order_date,
        quantity
    FROM {{ ref('stg_ct_dich_vu') }}
) AS sub_fact