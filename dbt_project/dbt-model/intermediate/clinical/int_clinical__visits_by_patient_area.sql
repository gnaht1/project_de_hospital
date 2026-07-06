with visits as (
    select * from {{ ref('int_clinical__patient_visits_classified') }}
),

addresses as (
    select * from {{ ref('stg_hospital_core__ct_address') }}
),

address_ranked as (
    select
        a.*,
        row_number() over (
            partition by a.nb_dot_dieu_tri_id
            order by coalesce(a.updated_at, a.created_at) desc
        ) as address_rank
    from addresses a
),

latest_address as (
    select *
    from address_ranked
    where address_rank = 1
),

xa_phuong as (
    select * from {{ ref('stg_hospital_core__dm_xa_phuong') }}
),

quan_huyen as (
    select * from {{ ref('stg_hospital_core__dm_quan_huyen') }}
),

tinh_thanh_pho as (
    select * from {{ ref('stg_hospital_core__dm_tinh_thanh_pho') }}
),

visits_enriched as (
    select
        v.dot_dieu_tri_id,
        v.nb_thong_tin_id,
        v.admission_time,
        v.created_at,
        v.visit_date,
        v.first_visit_date,
        v.is_new_patient,
        v.is_return_patient,

        a.xa_phuong_id,
        coalesce(xp.xa_phuong_name, 'Chua xac dinh') as xa_phuong_name,

        coalesce(a.quan_huyen_id, xp.quan_huyen_id) as quan_huyen_id,
        coalesce(qh.quan_huyen_name, 'Chua xac dinh') as quan_huyen_name,

        coalesce(a.tinh_thanh_pho_id, qh.tinh_thanh_pho_id) as tinh_thanh_pho_id,
        coalesce(tp.tinh_thanh_pho_name, 'Chua xac dinh') as tinh_thanh_pho_name,

        case
            when a.xa_phuong_id is null
             and a.quan_huyen_id is null
             and a.tinh_thanh_pho_id is null then false
            else true
        end as has_address
    from visits v
    left join latest_address a
        on v.dot_dieu_tri_id = a.nb_dot_dieu_tri_id
    left join xa_phuong xp
        on a.xa_phuong_id = xp.xa_phuong_id
    left join quan_huyen qh
        on coalesce(a.quan_huyen_id, xp.quan_huyen_id) = qh.quan_huyen_id
    left join tinh_thanh_pho tp
        on coalesce(a.tinh_thanh_pho_id, qh.tinh_thanh_pho_id) = tp.tinh_thanh_pho_id
)

select * from visits_enriched
