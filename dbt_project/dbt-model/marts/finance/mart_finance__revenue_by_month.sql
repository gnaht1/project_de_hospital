{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ky_doanh_thu',
    schema='gold_db'
) }}

SELECT *,
    CASE 
        WHEN doanh_thu_thang_truoc > 0
        THEN (tong_doanh_thu - doanh_thu_thang_truoc) / doanh_thu_thang_truoc
    END AS phan_tram_thay_doi
FROM (
    SELECT
        sub.*,
        LAG(sub.tong_doanh_thu) OVER (ORDER BY sub.ky_doanh_thu) AS doanh_thu_thang_truoc
    FROM (
        SELECT
            ky_doanh_thu,
            cast(date_trunc('month', thoi_gian_thanh_toan) as date) as stat_date,
            nam_doanh_thu as stat_year,
            thang_doanh_thu as stat_month,
            SUM(thanh_tien) AS tong_doanh_thu
        FROM {{ ref('int_finance__valid_payments') }}
        GROUP BY 1,2,3,4
    ) sub
) t
ORDER BY ky_doanh_thu;
