with enriched_episodes_today as (
    -- 1. Gọi bảng Silver ra và lọc lấy ngày đang test
    select * from {{ ref('int_clinical__enriched_episodes') }}
    where date_trunc('day', admission_time) = current_date()
),

technical_status as (
    -- 2. Aggregate technical service statuses into 4 main categories
    select 
        nb_dot_dieu_tri_id,
        
        -- Mapping logic: We assign a priority rank to each status code.
        -- The minimum rank (earliest stage) determines the overall patient status.
        -- Rank 1: Chờ Tiếp Nhận (20, 25)
        -- Rank 2: Đang Thực Hiện (40, 43, 46, 50, 60, 63, 66, 70, 80, 90)
        -- Rank 3: Có Kết Quả (100, 130, 140)
        -- Rank 4: Đã Duyệt (150, 155)
        case min(
            case 
                when trang_thai in (20, 25) then 1
                when trang_thai in (40, 43, 46, 50, 60, 63, 66, 70, 80, 90) then 2
                when trang_thai in (100, 130, 140) then 3
                when trang_thai in (150, 155) then 4
                else 5
            end
        )
            when 1 then 'Chờ Tiếp Nhận'
            when 2 then 'Đang Thực Hiện'
            when 3 then 'Có Kết Quả'
            when 4 then 'Đã Duyệt'
            else 'Khác'
        end as summary_status
        
    from {{ ref('stg_hospital_core__ct_dv_ky_thuat') }}
    
    -- Filter by clinical operational time to ensure accuracy
    where date_trunc('day', coalesce(thoi_gian_lay_so, thoi_gian_tiep_nhan)) = current_date()
    group by 1
)

select
    e.ma_hs,
    e.benh_nhan,
    e.khoa,
    e.doi_tuong,
    date_format(e.admission_time, 'HH:mm') as thoi_gian_vao,
    
    -- Lấy trạng thái, nếu NULL thì gán là Mới tiếp nhận
    coalesce(ts.summary_status, 'Mới tiếp nhận') as trang_thai

from enriched_episodes_today e
left join technical_status ts on e.nb_dot_dieu_tri_id = ts.nb_dot_dieu_tri_id

order by e.admission_time desc