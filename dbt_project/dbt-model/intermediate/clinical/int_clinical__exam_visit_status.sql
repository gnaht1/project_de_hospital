with exam_visits as (
    select * from {{ ref('stg_hospital_core__ct_dv_kham') }}
),

exam_conclusions as (
    select
        nb_dot_dieu_tri_id,
        max(thoi_gian_ket_luan) as latest_conclusion_time
    from {{ ref('stg_hospital_core__ct_dv_kham_ket_luan') }}
    group by 1
),

exam_status as (
    select
        ev.dv_kham_id,
        ev.nb_dot_dieu_tri_id,
        cast(date_trunc('day', ev.created_at) as date) as created_date,
        cast(date_trunc('day', ev.thoi_gian_kham) as date) as exam_date,
        cast(
            date_trunc(
                'day',
                coalesce(ec.latest_conclusion_time, ev.thoi_gian_ket_luan)
            ) as date
        ) as conclusion_date,
        ev.created_at,
        ev.thoi_gian_kham,
        coalesce(ec.latest_conclusion_time, ev.thoi_gian_ket_luan) as thoi_gian_ket_luan,
        case
            when coalesce(ec.latest_conclusion_time, ev.thoi_gian_ket_luan) is not null then 'Đã kết luận'
            when ev.thoi_gian_kham is not null then 'Đang khám'
            else 'Đang chờ khám'
        end as exam_status_label
    from exam_visits ev
    left join exam_conclusions ec
        on ev.nb_dot_dieu_tri_id = ec.nb_dot_dieu_tri_id
)

select * from exam_status
