with patient_sources as (
    select * from {{ ref('stg_hospital_core__ct_nguon_nb') }}
),

patient_source_dim as (
    select * from {{ ref('stg_hospital_core__dm_nguon_nb') }}
),

visits as (
    select * from {{ ref('int_clinical__patient_visits_classified') }}
),

ranked_sources as (
    select
        ps.*,
        row_number() over (
            partition by ps.nb_dot_dieu_tri_id
            order by coalesce(ps.updated_at, ps.created_at) desc
        ) as source_rank
    from patient_sources ps
    where ps.nguon_nb_id is not null
),

latest_patient_source as (
    select *
    from ranked_sources
    where source_rank = 1
),

attributed_visits as (
    select
        v.dot_dieu_tri_id,
        v.nb_thong_tin_id,
        v.admission_time,
        v.visit_date,
        v.is_new_patient,
        v.is_return_patient,

        ps.nguon_nb_id,
        s.code_nguon_nb,
        s.ten_nguon_nb,
        s.has_referrer,
        s.nhom_nguon,
        ps.nguoi_gioi_thieu_id,
        ps.created_at as source_created_at,
        ps.updated_at as source_updated_at
    from visits v
    inner join latest_patient_source ps
        on v.dot_dieu_tri_id = ps.nb_dot_dieu_tri_id
    left join patient_source_dim s
        on ps.nguon_nb_id = s.nguon_nb_id
    where s.ten_nguon_nb is not null
)

select * from attributed_visits
