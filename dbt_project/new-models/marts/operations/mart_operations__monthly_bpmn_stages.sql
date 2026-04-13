with tiep_don as (
    -- Stage 1: Reception (All regular visits by month)
    select
        extract(year from admission_time) as stat_year,
        extract(month from admission_time) as stat_month,
        date_trunc('month', admission_time) as stat_date,
        'Tiếp Đón & Khám Bệnh' as process_name,
        count(dot_dieu_tri_id) as total_count,
        1 as display_order
    from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
    where is_active = true
      and is_health_check = false
    group by 1, 2, 3
),

cls as (
    -- Stage 2: Paraclinical (Count distinct episodes having Lab/Imaging orders)
    select
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        date_trunc('month', order_time) as stat_date,
        'Cận Lâm Sàng' as process_name,
        count(distinct dot_dieu_tri_id) as total_count,
        2 as display_order
    from {{ ref('stg_hospital_core__ct_dich_vu') }}
    where is_active = true 
      and is_deleted = false
      and loai_dich_vu_id in (20, 30)
    group by 1, 2, 3
),

thuoc as (
    -- Stage 3: Pharmacy (Count distinct episodes having Medication orders)
    select
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        date_trunc('month', order_time) as stat_date,
        'Cấp Phát Thuốc' as process_name,
        count(distinct dot_dieu_tri_id) as total_count,
        3 as display_order
    from {{ ref('stg_hospital_core__ct_dich_vu') }}
    where is_active = true 
      and is_deleted = false
      and loai_dich_vu_id = 90
    group by 1, 2, 3
),

thu_ngan as (
    -- Stage 4: Cashier (Count distinct episodes generating financial transactions)
    select
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        date_trunc('month', order_time) as stat_date,
        'Thanh Toán & Thu Ngân' as process_name,
        count(distinct dot_dieu_tri_id) as total_count,
        4 as display_order
    from {{ ref('stg_hospital_core__ct_dich_vu') }}
    where is_active = true 
      and is_deleted = false
    group by 1, 2, 3
),

ksk as (
    -- Stage 5: Health Check
    select
        extract(year from admission_time) as stat_year,
        extract(month from admission_time) as stat_month,
        date_trunc('month', admission_time) as stat_date,
        'KSK Đoàn / Hợp Đồng' as process_name,
        count(dot_dieu_tri_id) as total_count,
        5 as display_order
    from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
    where is_active = true
      and is_health_check = true
    group by 1, 2, 3
),

combined_processes as (
    -- Combine all CTEs
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
    stat_year,
    stat_month,
    stat_date,
    'T' || lpad(cast(stat_month as string), 2, '0') as month_label,
    process_name,
    total_count,
    display_order
from combined_processes