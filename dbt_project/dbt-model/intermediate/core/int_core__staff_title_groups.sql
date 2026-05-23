with staff as (
    select * from {{ ref('stg_hospital_core__dm_nhan_vien') }}
),

titles as (
    select * from {{ ref('stg_hospital_core__dm_hoc_ham_hoc_vi') }}
),

grouped_staff as (
    select
        s.nhan_vien_id,
        s.doctor_name,
        s.hoc_ham_hoc_vi_id,
        t.title_code,
        t.title_name,
        case
            when upper(coalesce(t.title_code, '')) in (
                'PROF',
                'PROF_PHD',
                'ASSOC_PROF',
                'ASSOC_PROF_PHD',
                'PHD',
                'PHD_DR',
                'PHD_DR_SPEC_II'
            ) then 'PGS / TS'
            when upper(coalesce(t.title_code, '')) in (
                'MSC',
                'MSC_DR',
                'DR_SPEC_II',
                'MSC_DR_SPEC_II',
                'MSC_DR_SPEC_I'
            ) then 'ThS / BSCKII'
            when upper(coalesce(t.title_code, '')) in (
                'DR',
                'DR_SPEC_I',
                'RES_DR',
                'MSC_RES_DR'
            ) then 'BS / BSCKI'
            when upper(coalesce(t.title_code, '')) in (
                'PHAR_LVL_1',
                'PHAR_LVL_2',
                'NUR_LVL_1',
                'NUR_LVL_2'
            ) then 'Điều dưỡng / KTV'
            else 'Khác'
        end as title_group,
        case
            when upper(coalesce(t.title_code, '')) in (
                'PROF',
                'PROF_PHD',
                'ASSOC_PROF',
                'ASSOC_PROF_PHD',
                'PHD',
                'PHD_DR',
                'PHD_DR_SPEC_II'
            ) then 1
            when upper(coalesce(t.title_code, '')) in (
                'MSC',
                'MSC_DR',
                'DR_SPEC_II',
                'MSC_DR_SPEC_II',
                'MSC_DR_SPEC_I'
            ) then 2
            when upper(coalesce(t.title_code, '')) in (
                'DR',
                'DR_SPEC_I',
                'RES_DR',
                'MSC_RES_DR'
            ) then 3
            when upper(coalesce(t.title_code, '')) in (
                'PHAR_LVL_1',
                'PHAR_LVL_2',
                'NUR_LVL_1',
                'NUR_LVL_2'
            ) then 4
            else 5
        end as display_order
    from staff s
    left join titles t
        on s.hoc_ham_hoc_vi_id = t.hoc_ham_hoc_vi_id
)

select
    nhan_vien_id,
    doctor_name,
    hoc_ham_hoc_vi_id,
    title_code,
    title_name,
    title_group,
    display_order
from grouped_staff
