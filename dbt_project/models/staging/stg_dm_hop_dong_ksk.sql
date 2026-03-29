-- Extract active health checkup contracts
SELECT 
    id AS contract_id,
    -- Extract date from created_at timestamp string
    CAST(SUBSTR(created_at, 1, 10) AS DATE) AS created_date
FROM {{ source('core_his', 'dm_hop_dong_ksk_iceberg') }}
WHERE deleted = 0