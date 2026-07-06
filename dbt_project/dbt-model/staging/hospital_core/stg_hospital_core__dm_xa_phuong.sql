with source as (
    select * from {{ source('raw_hospital', 'dm_xa_phuong_iceberg') }}
),

renamed_and_casted as (
    select
        xa_phuong_id,
        ten_xa_phuong as xa_phuong_name,
        quan_huyen_id
    from source
    where xa_phuong_id is not null
)

select * from renamed_and_casted
