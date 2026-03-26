-- Create the receipt fact table for revenue aggregations
SELECT 
    sub_fact.receipt_id,
    sub_fact.encounter_id,
    sub_fact.cashier_key,
    sub_fact.patient_type_key,
    sub_fact.payment_date,
    sub_fact.total_amount
FROM (
    SELECT 
        receipt_id,
        encounter_id,
        cashier_key,
        patient_type_key,
        payment_date,
        total_amount
    FROM {{ ref('stg_ct_phieu_thu') }}
    -- Filter only paid receipts (assuming 1 means paid)
    WHERE payment_status = 50
) AS sub_fact