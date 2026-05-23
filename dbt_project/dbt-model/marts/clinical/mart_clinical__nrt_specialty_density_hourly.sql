{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='specialty_hour_key',
    file_format='iceberg',
    schema='gold_db'
) }}

with visits as (
    select
        dich_vu_id,
        order_time,
        specialty_code,
        specialty_name
    from {{ ref('int_clinical__specialty_visits') }}
    where order_time is not null

    {% if is_incremental() %}
      and order_time >= (
          select coalesce(max(stat_hour), cast('1900-01-01 00:00:00' as timestamp))
          from {{ this }}
      )
    {% endif %}
),

hourly_agg as (
    select
        cast(date_trunc('hour', order_time) as timestamp) as stat_hour,
        cast(date_trunc('day', order_time) as date) as stat_date,
        extract(hour from order_time) as hour_of_day,
        coalesce(specialty_code, 'UNKNOWN') as specialty_code,
        coalesce(specialty_name, 'Chưa xác định') as specialty_name,
        count(dich_vu_id) as total_visits
    from visits
    group by 1,2,3,4,5
)

select
    concat(cast(stat_hour as string), '||', specialty_code) as specialty_hour_key,
    stat_hour,
    stat_date,
    hour_of_day,
    specialty_code,
    specialty_name,
    total_visits
from hourly_agg
