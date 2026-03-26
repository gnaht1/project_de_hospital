-- Extract and clean receipt data, filtering out soft-deleted records
SELECT 
    sub_receipts.receipt_id,
    sub_receipts.encounter_id,
    sub_receipts.cashier_key,
    sub_receipts.patient_type_key,
    sub_receipts.payment_date,
    sub_receipts.total_amount,
    sub_receipts.payment_status
FROM (
    SELECT 
        id AS receipt_id,
        nb_dot_dieu_tri_id AS encounter_id,
        thu_ngan_id AS cashier_key,
        doi_tuong_kcb AS patient_type_key,
        -- Cast string time to date format for time-series aggregation
        CAST(SUBSTR(thoi_gian_thanh_toan, 1, 10) AS DATE) AS payment_date,
        thanh_tien AS total_amount,
        thanh_toan AS payment_status
    FROM {{ source('core_his', 'ct_phieu_thu_iceberg') }}
    WHERE deleted = 0 
      AND thoi_gian_thanh_toan IS NOT NULL
) AS sub_receipts