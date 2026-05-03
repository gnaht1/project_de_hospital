{{ config(
    materialized='table',
    file_format='iceberg',
    schema='gold_db'
) }}

with base as (
    select
        payment_time,
        net_revenue
    from {{ ref('mart_finance__nrt_revenue_receipts') }}
    where payment_time is not null
),

minute_agg as (
    select
        cast(date_trunc('minute', payment_time) as timestamp) as stat_minute,
        cast(date(payment_time) as date) as stat_date,
        extract(hour from payment_time) * 60 + extract(minute from payment_time) as minute_of_day,
        sum(net_revenue) as revenue_minute
    from base
    where date(payment_time) in (
        date_sub(current_date(), 1),
        current_date()
    )
    group by 1,2,3
),

pivoted as (
    select
        minute_of_day,
        max(case when stat_date = current_date() then revenue_minute end) as revenue_today_minute,
        max(case when stat_date = date_sub(current_date(), 1) then revenue_minute end) as revenue_yesterday_minute
    from minute_agg
    group by 1
),

final as (
    select
        minute_of_day,
        coalesce(revenue_today_minute, 0) as revenue_today_minute,
        coalesce(revenue_yesterday_minute, 0) as revenue_yesterday_minute,
        sum(coalesce(revenue_today_minute, 0)) over (
            order by minute_of_day
            rows between unbounded preceding and current row
        ) as revenue_today_cum,
        sum(coalesce(revenue_yesterday_minute, 0)) over (
            order by minute_of_day
            rows between unbounded preceding and current row
        ) as revenue_yesterday_cum
    from pivoted
)

select
    minute_of_day,
    lpad(cast(floor(minute_of_day / 60) as string), 2, '0')
    || ':' ||
    lpad(cast(minute_of_day % 60 as string), 2, '0') as hhmm_label,
    revenue_today_minute,
    revenue_yesterday_minute,
    revenue_today_cum,
    revenue_yesterday_cum
from final
order by minute_of_day
