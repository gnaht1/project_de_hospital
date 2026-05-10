with exam_status as (
    select * from {{ ref('int_clinical__exam_visit_status') }}
),

cls_orders as (
    select
        cast(date_trunc('day', coalesce(thoi_gian_lay_so, thoi_gian_tiep_nhan, created_at)) as date) as metric_date,
        count(distinct nb_dot_dieu_tri_id) as cls_order_count
    from {{ ref('stg_hospital_core__ct_dv_ky_thuat') }}
    group by 1
),

calendar_dates as (
    select created_date as metric_date from exam_status where created_date is not null
    union
    select exam_date as metric_date from exam_status where exam_date is not null
    union
    select conclusion_date as metric_date from exam_status where conclusion_date is not null
    union
    select metric_date from cls_orders where metric_date is not null
),

waiting_daily as (
    select
        created_date as metric_date,
        count(distinct nb_dot_dieu_tri_id) as waiting_for_exam_count
    from exam_status
    where created_date is not null
      and thoi_gian_kham is null
      and thoi_gian_ket_luan is null
    group by 1
),

in_exam_daily as (
    select
        exam_date as metric_date,
        count(distinct nb_dot_dieu_tri_id) as in_exam_count
    from exam_status
    where exam_date is not null
      and thoi_gian_kham is not null
      and thoi_gian_ket_luan is null
    group by 1
),

concluded_daily as (
    select
        conclusion_date as metric_date,
        count(distinct nb_dot_dieu_tri_id) as concluded_count
    from exam_status
    where conclusion_date is not null
    group by 1
)

select
    c.metric_date,
    coalesce(w.waiting_for_exam_count, 0) as waiting_for_exam_count,
    coalesce(i.in_exam_count, 0) as in_exam_count,
    coalesce(cd.concluded_count, 0) as concluded_count,
    coalesce(cls.cls_order_count, 0) as cls_order_count
from calendar_dates c
left join waiting_daily w
    on c.metric_date = w.metric_date
left join in_exam_daily i
    on c.metric_date = i.metric_date
left join concluded_daily cd
    on c.metric_date = cd.metric_date
left join cls_orders cls
    on c.metric_date = cls.metric_date
