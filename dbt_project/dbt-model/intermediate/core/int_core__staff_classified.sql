with stg_nhan_vien as (
    select * from {{ ref('stg_hospital_core__dm_nhan_vien') }}
),

stg_hoc_ham as (
    select * from {{ ref('stg_hospital_core__dm_hoc_ham_hoc_vi') }}
),

classified_staff as (
    select
        nv.nhan_vien_id,
        nv.joined_at,
        -- Identify doctors based on title name or title code
        case 
            when lower(hh.title_code) like '%dr%' 
            then true 
            else false 
        end as is_doctor
    from stg_nhan_vien nv
    left join stg_hoc_ham hh
        on nv.hoc_ham_hoc_vi_id = hh.hoc_ham_hoc_vi_id
    where nv.joined_at is not null
)

select * from classified_staff