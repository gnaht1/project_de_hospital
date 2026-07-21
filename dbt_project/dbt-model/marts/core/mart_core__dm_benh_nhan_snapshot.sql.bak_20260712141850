{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='nb_thong_tin_id',
    file_format='iceberg'
) }}

with bronze_src as (
    select
        nb_thong_tin_id,
        ma_nb,
        email,
        ngay_sinh,
        noi_lam_viec,
        so_dien_thoai,
        ten_nb,
        ten_nb_khong_dau,
        created_at,
        updated_at,
        op,
        ts_ms
    from {{ source('raw_hospital', 'dm_benh_nhan_iceberg') }}

    {% if is_incremental() %}
      where cast(ts_ms as bigint) > (
          select coalesce(max(cast(ts_ms as bigint)), 0)
          from {{ this }}
      )
    {% endif %}
),

dedup_latest as (
    select
        nb_thong_tin_id,
        ma_nb,
        email,
        ngay_sinh,
        noi_lam_viec,
        so_dien_thoai,
        ten_nb,
        ten_nb_khong_dau,
        created_at,
        updated_at,
        cast(ts_ms as bigint) as ts_ms,
        row_number() over (
            partition by nb_thong_tin_id
            order by cast(ts_ms as bigint) desc
        ) as rn,
        op
    from bronze_src
)

select
    nb_thong_tin_id,
    ma_nb,
    email,
    ngay_sinh,
    noi_lam_viec,
    so_dien_thoai,
    ten_nb,
    ten_nb_khong_dau,
    created_at,
    updated_at,
    ts_ms
from dedup_latest
where rn = 1
  and coalesce(op, 'u') <> 'd'
