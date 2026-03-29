-- Extract academic titles/degrees
SELECT 
    id AS degree_id,
    ten AS degree_name,
    code_hoc_ham AS degree_code
FROM {{ source('core_his', 'dm_hoc_ham_hoc_vi_iceberg') }}
WHERE deleted = 0