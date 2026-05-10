with date_window_options as (
    select 'TODAY' as date_window_key, 'Hôm nay' as date_window_label, 1 as window_days, 1 as sort_order
    union all
    select 'LAST_7_DAYS' as date_window_key, '7 ngày' as date_window_label, 7 as window_days, 2 as sort_order
    union all
    select 'LAST_30_DAYS' as date_window_key, '30 ngày' as date_window_label, 30 as window_days, 3 as sort_order
),

tiep_don as (
    select
        w.date_window_key,
        w.date_window_label,
        w.sort_order,
        extract(year from d.admission_time) as stat_year,
        extract(month from d.admission_time) as stat_month,
        extract(day from d.admission_time) as stat_day,
        date_trunc('day', d.admission_time) as stat_date,
        'Tiếp Đón & Khám Bệnh' as process_name,
        count(d.dot_dieu_tri_id) as total_count,
        1 as display_order
    from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }} d
    cross join date_window_options w
    where d.is_active = true
      and d.is_health_check = false
      and datediff(current_date, cast(d.admission_time as date)) between 0 and w.window_days - 1
    group by 1, 2, 3, 4, 5, 6, 7
),

cls as (
    select
        w.date_window_key,
        w.date_window_label,
        w.sort_order,
        extract(year from dv.order_time) as stat_year,
        extract(month from dv.order_time) as stat_month,
        extract(day from dv.order_time) as stat_day,
        date_trunc('day', dv.order_time) as stat_date,
        'Cận Lâm Sàng' as process_name,
        count(distinct dv.dot_dieu_tri_id) as total_count,
        2 as display_order
    from {{ ref('stg_hospital_core__ct_dich_vu') }} dv
    cross join date_window_options w
    where dv.is_active = true
      and dv.is_deleted = false
      and dv.loai_dich_vu_id in (20, 30)
      and datediff(current_date, cast(dv.order_time as date)) between 0 and w.window_days - 1
    group by 1, 2, 3, 4, 5, 6, 7
),

thuoc as (
    select
        w.date_window_key,
        w.date_window_label,
        w.sort_order,
        extract(year from dv.order_time) as stat_year,
        extract(month from dv.order_time) as stat_month,
        extract(day from dv.order_time) as stat_day,
        date_trunc('day', dv.order_time) as stat_date,
        'Cấp Phát Thuốc' as process_name,
        count(distinct dv.dot_dieu_tri_id) as total_count,
        3 as display_order
    from {{ ref('stg_hospital_core__ct_dich_vu') }} dv
    cross join date_window_options w
    where dv.is_active = true
      and dv.is_deleted = false
      and dv.loai_dich_vu_id = 90
      and datediff(current_date, cast(dv.order_time as date)) between 0 and w.window_days - 1
    group by 1, 2, 3, 4, 5, 6, 7
),

thu_ngan as (
    select
        w.date_window_key,
        w.date_window_label,
        w.sort_order,
        extract(year from dv.order_time) as stat_year,
        extract(month from dv.order_time) as stat_month,
        extract(day from dv.order_time) as stat_day,
        date_trunc('day', dv.order_time) as stat_date,
        'Thanh Toán & Thu Ngân' as process_name,
        count(distinct dv.dot_dieu_tri_id) as total_count,
        4 as display_order
    from {{ ref('stg_hospital_core__ct_dich_vu') }} dv
    cross join date_window_options w
    where dv.is_active = true
      and dv.is_deleted = false
      and datediff(current_date, cast(dv.order_time as date)) between 0 and w.window_days - 1
    group by 1, 2, 3, 4, 5, 6, 7
),

ksk as (
    select
        w.date_window_key,
        w.date_window_label,
        w.sort_order,
        extract(year from d.admission_time) as stat_year,
        extract(month from d.admission_time) as stat_month,
        extract(day from d.admission_time) as stat_day,
        date_trunc('day', d.admission_time) as stat_date,
        'KSK Đoàn / Hợp Đồng' as process_name,
        count(d.dot_dieu_tri_id) as total_count,
        5 as display_order
    from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }} d
    cross join date_window_options w
    where d.is_active = true
      and d.is_health_check = true
      and datediff(current_date, cast(d.admission_time as date)) between 0 and w.window_days - 1
    group by 1, 2, 3, 4, 5, 6, 7
),

combined_processes as (
    select * from tiep_don
    union all
    select * from cls
    union all
    select * from thuoc
    union all
    select * from thu_ngan
    union all
    select * from ksk
)

select
    date_window_key,
    date_window_label,
    sort_order,
    stat_year,
    stat_month,
    stat_day,
    stat_date,
    'T' || lpad(cast(stat_month as string), 2, '0') as month_label,
    cast(stat_date as string) as day_label,
    process_name,
    total_count,
    display_order
from combined_processes