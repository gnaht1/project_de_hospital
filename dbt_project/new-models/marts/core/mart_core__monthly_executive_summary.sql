with 
-- 1. Revenue
cte_doanh_thu as (
    select
        extract(year from cast(thoi_gian_thanh_toan as timestamp)) as stat_year,
        extract(month from cast(thoi_gian_thanh_toan as timestamp)) as stat_month,
        -- Convert to Million VND
        sum(thanh_tien) / 1000000.0 as gia_tri
    from {{ ref('stg_hospital_core__ct_phieu_thu') }}
    where thoi_gian_thanh_toan is not null
    group by 1, 2
),

-- 2. Total Visits
cte_luot_kham as (
    select
        extract(year from admission_time) as stat_year,
        extract(month from admission_time) as stat_month,
        count(dot_dieu_tri_id) as gia_tri
    from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
    group by 1, 2
),

-- 3. Paraclinical Services (Lab & Imaging)
cte_dich_vu_cls as (
    select
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        count(dich_vu_id) as gia_tri
    from {{ ref('stg_hospital_core__ct_dich_vu') }}
    where loai_dich_vu_id in (20, 30)
    group by 1, 2
),

-- 4. Prescriptions
cte_toa_thuoc as (
    select
        extract(year from order_time) as stat_year,
        extract(month from order_time) as stat_month,
        count(dich_vu_id) as gia_tri
    from {{ ref('stg_hospital_core__ct_dich_vu') }}
    where loai_dich_vu_id = 90
    group by 1, 2
),

-- 5. New Patients
cte_bn_first_visit as (
    select 
        ma_nb,
        min(admission_time) as first_visit_time
    from {{ ref('stg_hospital_core__ct_dot_dieu_tri') }}
    where ma_nb is not null
    group by ma_nb
),
cte_bn_moi as (
    select
        extract(year from first_visit_time) as stat_year,
        extract(month from first_visit_time) as stat_month,
        count(ma_nb) as gia_tri
    from cte_bn_first_visit
    group by 1, 2
),

-- 6. Health Check Contracts
cte_hd_ksk as (
    select
        extract(year from ngay_hieu_luc) as stat_year,
        extract(month from ngay_hieu_luc) as stat_month,
        count(hop_dong_id) as gia_tri
    from {{ ref('stg_hospital_core__dm_hop_dong_ksk') }}
    group by 1, 2
),

-- ==========================================
-- UNPIVOT: Combine into a single vertical table
-- ==========================================
unpivoted_metrics as (
    select stat_year, stat_month, '1. Doanh Thu (Triệu)' as chi_so, gia_tri from cte_doanh_thu
    union all
    select stat_year, stat_month, '2. Lượt Khám' as chi_so, gia_tri from cte_luot_kham
    union all
    select stat_year, stat_month, '3. Dịch Vụ CLS' as chi_so, gia_tri from cte_dich_vu_cls
    union all
    select stat_year, stat_month, '4. Toa Thuốc' as chi_so, gia_tri from cte_toa_thuoc
    union all
    select stat_year, stat_month, '5. BN Mới' as chi_so, gia_tri from cte_bn_moi
    union all
    select stat_year, stat_month, '6. HĐ KSK' as chi_so, gia_tri from cte_hd_ksk
)

-- Format month for Superset display
select
    stat_year,
    stat_month,
    'T' || cast(stat_month as string) as month_label,
    chi_so,
    gia_tri
from unpivoted_metrics