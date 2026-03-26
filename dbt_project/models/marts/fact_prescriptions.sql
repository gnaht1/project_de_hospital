-- Create fact table specifically for dispensed prescriptions (medicines)
SELECT 
    sub_fact.service_order_id,
    sub_fact.encounter_id,
    sub_fact.execution_date,
    sub_fact.quantity
FROM (
    SELECT 
        service_order_id,
        encounter_id,
        execution_date,
        quantity
    FROM {{ ref('stg_ct_dich_vu') }}
    -- Filter for Medicine (ID = 90) and ensure it has been dispensed
    WHERE service_category_id = 90 
      AND execution_date IS NOT NULL
) AS sub_fact