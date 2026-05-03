{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='phieu_thu_id',
    file_format='iceberg',
    schema='gold_db'
) }}

with src as (
    select
        id as phieu_thu_id,
        nb_dot_dieu_tri_id,
        cast(thoi_gian_thanh_toan as timestamp) as payment_time,
        cast(date_trunc('minute', thoi_gian_thanh_toan) as timestamp) as payment_minute,
        cast(date_trunc('hour', thoi_gian_thanh_toan) as timestamp) as payment_hour,
        cast(date_trunc('day', thoi_gian_thanh_toan) as date) as payment_date,
        coalesce(thanh_tien, 0) as gross_revenue,
        coalesce(tien_hoan_tra, 0) as refund_amount,
        coalesce(thanh_tien, 0) - coalesce(tien_hoan_tra, 0) as net_revenue,
        cast(ts_ms as bigint) as ts_ms,
        op,
        row_number() over (
            partition by id
            order by cast(ts_ms as bigint) desc
        ) as rn
    from {{ source('raw_hospital', 'ct_phieu_thu_iceberg') }}
    where active = true
      and deleted = 0
      and thanh_toan = 50
      and thoi_gian_thanh_toan is not null

    {% if is_incremental() %}
      and cast(ts_ms as bigint) > (
        select coalesce(max(ts_ms), 0) from {{ this }}
      )
    {% endif %}
),

latest as (
    select
        phieu_thu_id,
        nb_dot_dieu_tri_id,
        payment_time,
        payment_minute,
        payment_hour,
        payment_date,
        gross_revenue,
        refund_amount,
        net_revenue,
        ts_ms
    from src
    where rn = 1
      and coalesce(op, 'u') <> 'd'
)

select * from latest
