with stg_dich_vu as (
    select * from {{ ref('stg_hospital_core__ct_dich_vu') }}
    -- Filter for Paraclinical services only (Lab & Imaging)
    where loai_dich_vu_id in (20, 30)
),

stg_dv_ky_thuat as (
    select * from {{ ref('stg_hospital_core__ct_dv_ky_thuat') }}
),

stg_phong as (
    select * from {{ ref('stg_hospital_core__dm_phong') }}
),

joined_data as (
    select
        dv.dich_vu_id,
        dv.order_time,
        
        -- Categorize statuses based on volume distribution analysis
        case 
            -- The massive volumes (150, 155) represent the final completed states
            when kt.trang_thai in (150, 155) then 'Hoàn thành'
            
            -- The smaller volumes represent the intermediate pipeline
            when kt.trang_thai < 150 then 'Đang chờ'
            
            else 'Khác'
        end as execution_status,
        
        coalesce(p.room_name, 'Chưa xác định') as room_name
        
    from stg_dich_vu dv
    inner join stg_dv_ky_thuat kt on dv.dich_vu_id = kt.dich_vu_id
    left join stg_phong p on kt.phong_thuc_hien_id = p.phong_id
    
    -- Removed the "< 100" filter since we now know > 100 are valid states
)

select * from joined_data