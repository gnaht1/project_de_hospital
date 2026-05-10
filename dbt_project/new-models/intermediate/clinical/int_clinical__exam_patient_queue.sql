with exam_status as (
    select * from {{ ref('int_clinical__exam_visit_status') }}
),

exam_visits as (
    select * from {{ ref('stg_hospital_core__ct_dv_kham') }}
),

episodes as (
    select * from {{ ref('int_clinical__enriched_episodes') }}
),

doctors as (
    select
        nhan_vien_id,
        doctor_name
    from {{ ref('stg_hospital_core__dm_nhan_vien') }}
),

cls_summary as (
    select
        nb_dot_dieu_tri_id,
        max(
            case
                when trang_thai in (20, 25, 40, 43, 46, 50, 60, 63, 66, 70, 80, 90) then 1
                else 0
            end
        ) as has_pending_cls
    from {{ ref('stg_hospital_core__ct_dv_ky_thuat') }}
    group by 1
),

enriched_queue as (
    select
        es.dv_kham_id,
        es.nb_dot_dieu_tri_id,
        coalesce(es.created_date, es.exam_date, es.conclusion_date) as metric_date,
        ep.ma_hs as patient_code,
        ep.benh_nhan as patient_name,
        coalesce(dk.doctor_name, dkl.doctor_name, 'Không rõ') as examining_doctor_name,
        ep.khoa as department_name,
        coalesce(ev.thoi_gian_kham, es.created_at) as exam_display_time,
        case
            when es.thoi_gian_ket_luan is not null then 'Đã kết luận'
            when coalesce(cs.has_pending_cls, 0) = 1 and es.thoi_gian_kham is not null then 'Chờ kết quả CLS'
            when es.thoi_gian_kham is not null then 'Đang khám'
            else 'Chờ khám'
        end as status_label,
        case
            when es.thoi_gian_ket_luan is not null then 4
            when coalesce(cs.has_pending_cls, 0) = 1 and es.thoi_gian_kham is not null then 3
            when es.thoi_gian_kham is not null then 2
            else 1
        end as status_order
    from exam_status es
    left join exam_visits ev
        on es.dv_kham_id = ev.dv_kham_id
    left join episodes ep
        on es.nb_dot_dieu_tri_id = ep.nb_dot_dieu_tri_id
    left join doctors dk
        on ev.bac_si_kham_id = dk.nhan_vien_id
    left join doctors dkl
        on ev.bac_si_ket_luan_id = dkl.nhan_vien_id
    left join cls_summary cs
        on es.nb_dot_dieu_tri_id = cs.nb_dot_dieu_tri_id
)

select
    dv_kham_id,
    nb_dot_dieu_tri_id,
    metric_date,
    patient_code,
    patient_name,
    examining_doctor_name,
    department_name,
    exam_display_time,
    status_label,
    status_order
from enriched_queue
where metric_date is not null
