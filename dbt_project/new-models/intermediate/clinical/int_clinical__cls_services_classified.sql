with stg_dich_vu as (
    select * from {{ ref('stg_hospital_core__ct_dich_vu') }}
),

stg_loai_dich_vu as (
    select * from {{ ref('stg_hospital_core__dm_loai_dich_vu') }}
),

classified_services as (
    select
        dv.dich_vu_id,
        dv.order_time,
        
        extract(month from dv.order_time) as order_month_num,
        extract(year from dv.order_time) as order_year,
        
        -- Categorize based on the provided IDs
        case 
            when ldv.loai_dich_vu_id = 20 then 'Xét Nghiệm'
            when ldv.loai_dich_vu_id = 30 then 'CĐHA'
            else 'Khác' 
        end as service_group
        
    from stg_dich_vu dv
    left join stg_loai_dich_vu ldv
        on dv.loai_dich_vu_id = ldv.loai_dich_vu_id
    where dv.order_time is not null
)

select * from classified_services