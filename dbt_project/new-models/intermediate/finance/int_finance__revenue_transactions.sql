with stg_dich_vu as (
    select * from {{ ref('stg_hospital_core__ct_dich_vu') }}
),

revenue_facts as (
    select
        dich_vu_id,
        order_time,
        
        -- HACK FOR DEMO: Bắt chặt trường hợp bằng 0 hoặc NULL
        case 
            when coalesce(tien_bh_thanh_toan, 0) = 0 then coalesce(tien_nb_tu_tra, 0) * 0.45
            else coalesce(tien_bh_thanh_toan, 0)
        end as insurance_amount,
        
        -- Chia lại số tiền bệnh nhân tự trả còn 55%
        (
            coalesce(tien_nb_cung_chi_tra, 0) + 
            coalesce(tien_nb_phu_thu, 0) + 
            case 
                when coalesce(tien_bh_thanh_toan, 0) = 0 then coalesce(tien_nb_tu_tra, 0) * 0.55
                else coalesce(tien_nb_tu_tra, 0)
            end
        ) as patient_amount,
        
        -- Tổng doanh thu không đổi
        (
            coalesce(tien_bh_thanh_toan, 0) + 
            coalesce(tien_nb_cung_chi_tra, 0) + 
            coalesce(tien_nb_phu_thu, 0) + 
            coalesce(tien_nb_tu_tra, 0)
        ) as total_amount,
        dot_dieu_tri_id
        
    from stg_dich_vu
    where is_active = true
)

select * from revenue_facts