#!/bin/bash

# Khai báo môi trường
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
SPARK_SQL="/opt/spark/bin/spark-sql"

echo "--- [$(date)] BẮT ĐẦU BẢO TRÌ LAKEHOUSE (RAW LAYER) ---"

echo "1. Đang tạm dừng main_job.py và giải phóng RAM..."
pkill -f main_job.py
pkill -f "Hospital_CDC"
sleep 5
rm -rf /root/metastore_db/db.lck

# Tính ngày giờ của 3 ngày trước
EXPIRE_DATE=$(date -d "3 days ago" +"%Y-%m-%d %H:%M:%S")
echo "=> Thời gian sẽ xóa Snapshot: trước $EXPIRE_DATE"

# Khai báo danh sách các bảng RAW cần bảo trì
TABLES=(
  "db.dm_khoa_iceberg"
  "db.dm_loai_dich_vu_iceberg"
  "db.ct_address_iceberg"
  "db.ct_bo_chi_dinh_iceberg"
  "db.ct_dich_vu_iceberg"
  "db.ct_dot_dieu_tri_iceberg"
  "db.ct_dv_kham_iceberg"
  "db.ct_dv_kham_ket_luan_iceberg"
  "db.ct_dv_ky_thuat_iceberg"
  "db.ct_kham_suc_khoe_iceberg"
  "db.ct_nguon_nb_iceberg"
  "db.ct_phieu_thu_iceberg"
  "db.dm_benh_nhan_iceberg"
  "db.dm_bo_chi_dinh_iceberg"
  "db.dm_chuyen_khoa_iceberg"
  "db.dm_dich_vu_iceberg"
  "db.dm_doi_tuong_kcb_iceberg"
  "db.dm_dv_discount_iceberg"
  "db.dm_hoc_ham_hoc_vi_iceberg"
  "db.dm_hop_dong_ksk_iceberg"
  "db.dm_nguoi_gioi_thieu_iceberg"
  "db.dm_nguon_nb_iceberg"
  "db.dm_nhan_vien_iceberg"
  "db.dm_nhom_dich_vu_cap1_iceberg"
  "db.dm_nhom_dich_vu_cap2_iceberg"
  "db.dm_nhom_dich_vu_cap3_iceberg"
  "db.dm_phong_iceberg"
  "db.dm_quan_huyen_iceberg"
  "db.dm_tinh_thanh_pho_iceberg"
  "db.dm_xa_phuong_iceberg"
  "db.hospital_configs_iceberg"
)

# TẠO FILE SQL ĐỘNG ĐỂ CHẠY 1 LẦN DUY NHẤT (Cứu tinh cho CPU/RAM)
TEMP_SQL_FILE="/tmp/run_maintenance_job.sql"
> $TEMP_SQL_FILE # Xóa trắng file tạm

for table in "${TABLES[@]}"; do
    echo "CALL his_catalog.system.rewrite_data_files(table => '${table}');" >> $TEMP_SQL_FILE
    echo "CALL his_catalog.system.rewrite_manifests(table => '${table}');" >> $TEMP_SQL_FILE
    echo "CALL his_catalog.system.expire_snapshots(table => '${table}', older_than => TIMESTAMP '${EXPIRE_DATE}');" >> $TEMP_SQL_FILE
done

echo "2. Đang thực thi lệnh bảo trì Iceberg..."

# Chạy Spark-SQL 1 lần duy nhất để đọc file SQL vừa tạo
$SPARK_SQL \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.postgresql:postgresql:42.6.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.his_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.his_catalog.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
  --conf spark.sql.catalog.his_catalog.uri=jdbc:postgresql://172.30.2.170:5432/iceberg_catalog \
  --conf spark.sql.catalog.his_catalog.jdbc.user=data_admin \
  --conf spark.sql.catalog.his_catalog.jdbc.password=12345678 \
  --conf spark.sql.catalog.his_catalog.warehouse=s3a://hospital-datalake/iceberg_warehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://172.30.2.170:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=12345678 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.driver.memory=2g \
  --conf spark.executor.memory=2g \
  --conf spark.hadoop.javax.jdo.option.ConnectionURL="jdbc:derby:;databaseName=/tmp/maintenance_derby;create=true" \
  -f $TEMP_SQL_FILE

echo "3. Dọn dẹp xong. Đang khởi động lại Spark Streaming..."
# Lưu ý: Chỉnh lại đường dẫn tới file main_job.py cho đúng với hệ thống của bạn
nohup /opt/spark/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.postgresql:postgresql:42.6.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /root/hospital_de_project/main_job.py > /root/hospital_de_project/streaming_job.log 2>&1 &

echo "--- [$(date)] BẢO TRÌ HOÀN TẤT ---"