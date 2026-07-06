with visits_by_area as (
    select * from {{ ref('int_clinical__visits_by_patient_area') }}
),

monthly_area as (
    select
        extract(year from admission_time) as stat_year,
        extract(month from admission_time) as stat_month,
        date_trunc('month', cast(admission_time as timestamp)) as stat_date,
        'T' || lpad(cast(extract(month from admission_time) as string), 2, '0') as month_label,

        tinh_thanh_pho_id,
        tinh_thanh_pho_name,
        quan_huyen_id,
        quan_huyen_name,
        xa_phuong_id,
        xa_phuong_name,
        has_address,

        concat(coalesce(tinh_thanh_pho_name, 'Chua xac dinh'), ' / ', coalesce(quan_huyen_name, 'Chua xac dinh')) as province_district_label,
        concat(
            coalesce(tinh_thanh_pho_name, 'Chua xac dinh'),
            ' / ',
            coalesce(quan_huyen_name, 'Chua xac dinh'),
            ' / ',
            coalesce(xa_phuong_name, 'Chua xac dinh')
        ) as full_area_label,

        count(dot_dieu_tri_id) as total_visits,
        count(distinct nb_thong_tin_id) as unique_patients,
        sum(is_new_patient) as new_patient_visits,
        sum(is_return_patient) as return_patient_visits
    from visits_by_area
    group by
        extract(year from admission_time),
        extract(month from admission_time),
        date_trunc('month', cast(admission_time as timestamp)),
        'T' || lpad(cast(extract(month from admission_time) as string), 2, '0'),
        tinh_thanh_pho_id,
        tinh_thanh_pho_name,
        quan_huyen_id,
        quan_huyen_name,
        xa_phuong_id,
        xa_phuong_name,
        has_address,
        concat(coalesce(tinh_thanh_pho_name, 'Chua xac dinh'), ' / ', coalesce(quan_huyen_name, 'Chua xac dinh')),
        concat(
            coalesce(tinh_thanh_pho_name, 'Chua xac dinh'),
            ' / ',
            coalesce(quan_huyen_name, 'Chua xac dinh'),
            ' / ',
            coalesce(xa_phuong_name, 'Chua xac dinh')
        )
)

select *
from monthly_area
