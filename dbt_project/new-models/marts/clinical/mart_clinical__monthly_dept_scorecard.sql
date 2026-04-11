with stg_khoa as (
    select * from {{ ref('stg_hospital_core__dm_khoa') }}
),

revenue_metrics as (
    -- Calculate total revenue per department per month
    select
        extract(year from thoi_gian_thanh_toan) as stat_year,
        extract(month from thoi_gian_thanh_toan) as stat_month,
        department_name,
        sum(doanh_thu_dich_vu) / 1000000.0 as metric_value
    from {{ ref('int_finance__revenue_by_department') }}
    group by 1, 2, 3
),

visit_metrics as (
    -- Count total examinations mapped to Department (Khoa Chỉ Định)
    select
        extract(year from dv.order_time) as stat_year,
        extract(month from dv.order_time) as stat_month,
        k.department_name,
        count(dv.dich_vu_id) as metric_value
    from {{ ref('stg_hospital_core__ct_dich_vu') }} dv
    inner join stg_khoa k on dv.khoa_chi_dinh_id = k.khoa_id
    where dv.loai_dich_vu_id = 10
    group by 1, 2, 3
),

cls_metrics as (
    -- Count total paraclinical services mapped to Department
    select
        extract(year from dv.order_time) as stat_year,
        extract(month from dv.order_time) as stat_month,
        k.department_name,
        count(dv.dich_vu_id) as metric_value
    from {{ ref('stg_hospital_core__ct_dich_vu') }} dv
    inner join stg_khoa k on dv.khoa_chi_dinh_id = k.khoa_id
    where dv.loai_dich_vu_id in (20, 30)
    group by 1, 2, 3
),

unioned_metrics as (
    -- Union all metrics vertically to avoid NULL join issues
    select stat_year, stat_month, department_name, metric_value as revenue, 0 as visits, 0 as cls_volume from revenue_metrics
    union all
    select stat_year, stat_month, department_name, 0, metric_value, 0 from visit_metrics
    union all
    select stat_year, stat_month, department_name, 0, 0, metric_value from cls_metrics
),

scorecard as (
    -- Aggregate the unioned data into a single row per department per month
    select
        stat_year,
        stat_month,
        'T' || lpad(cast(stat_month as string), 2, '0') as month_label,
        department_name,
        sum(revenue) as revenue,
        sum(visits) as visits,
        sum(cls_volume) as cls_volume,
        
        -- Dummy metrics for demonstration
        round(sum(visits) * 0.8) as prescriptions,
        round(sum(visits) * 0.3) as new_patients
    from unioned_metrics
    group by 1, 2, 3, 4
),

final_ranking as (
    select
        *,
        -- Generate Star icons directly in SQL for Superset UI
        case 
            when revenue > 500 then '⭐⭐⭐⭐⭐'
            when revenue > 300 then '⭐⭐⭐⭐'
            when revenue > 150 then '⭐⭐⭐'
            when revenue > 50 then '⭐⭐'
            else '⭐'
        end as efficiency_stars
        
    from scorecard
    -- Only show departments with actual activity
    where revenue > 0 or visits > 0 or cls_volume > 0
)

select * from final_ranking