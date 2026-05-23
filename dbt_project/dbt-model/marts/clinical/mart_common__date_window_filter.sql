with date_window_options as (
    select 'TODAY' as date_window_key, 'Hôm nay' as date_window_label, 1 as window_days, 1 as sort_order
    union all
    select 'LAST_7_DAYS' as date_window_key, '7 ngày' as date_window_label, 7 as window_days, 2 as sort_order
    union all
    select 'LAST_30_DAYS' as date_window_key, '30 ngày' as date_window_label, 30 as window_days, 3 as sort_order
)

select
    date_window_key,
    date_window_label,
    window_days,
    sort_order
from date_window_options
