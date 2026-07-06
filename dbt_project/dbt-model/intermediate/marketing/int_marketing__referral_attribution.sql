with patient_sources as (
    select * from {{ ref('stg_hospital_core__ct_nguon_nb') }}
),

referrers as (
    select * from {{ ref('stg_hospital_core__dm_nguoi_gioi_thieu') }}
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
    where ps.nguoi_gioi_thieu_id is not null
),

latest_referral_source as (
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

        rs.nguoi_gioi_thieu_id,
        r.code_nguoi_gioi_thieu,
        r.ten_nguoi_gioi_thieu,
        rs.nguon_nb_id,
        s.code_nguon_nb,
        s.ten_nguon_nb,
        rs.created_at as referral_created_at,
        rs.updated_at as referral_updated_at
    from visits v
    inner join latest_referral_source rs
        on v.dot_dieu_tri_id = rs.nb_dot_dieu_tri_id
    left join referrers r
        on rs.nguoi_gioi_thieu_id = r.nguoi_gioi_thieu_id
    left join patient_source_dim s
        on rs.nguon_nb_id = s.nguon_nb_id
    where r.ten_nguoi_gioi_thieu is not null
)

select * from attributed_visits
