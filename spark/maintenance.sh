#!/bin/bash

# ==============================
# Iceberg Lakehouse Maintenance
# Safe hourly version for small VPS
# ==============================

set -o pipefail

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
SPARK_SQL="/opt/spark/bin/spark-sql"

LOG_PREFIX="--- [$(date '+%Y-%m-%d %H:%M:%S')]"

echo "$LOG_PREFIX BẮT ĐẦU BẢO TRÌ LAKEHOUSE ---"

# Snapshot có thể dọn ngắn để giảm metadata nhanh
SNAPSHOT_EXPIRE_DATE=$(date -d "2 hours ago" +"%Y-%m-%d %H:%M:%S")

# Orphan files bắt buộc nên >= 24h, Iceberg sẽ chặn nếu thấp hơn
ORPHAN_EXPIRE_DATE=$(date -d "25 hours ago" +"%Y-%m-%d %H:%M:%S")

echo "=> Sẽ xóa snapshots cũ hơn: $SNAPSHOT_EXPIRE_DATE"
echo "=> Sẽ xóa orphan files cũ hơn: $ORPHAN_EXPIRE_DATE"

# File SQL tạm
TEMP_SQL_FILE="/tmp/run_iceberg_maintenance.sql"
> "$TEMP_SQL_FILE"

# ==============================
# 1. Các bảng RAW / Bronze
# ==============================

RAW_TABLES=(
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

# ==============================
# 2. Các bảng dbt / NRT dễ phình metadata
# ==============================

HOT_TABLES=(
  "gold_db.mart_core__dm_benh_nhan_snapshot"
  "gold_db.mart_finance__nrt_revenue_receipts"
  "gold_db.mart_finance__nrt_revenue_today_vs_yesterday"
  "gold_db.mart_clinical__nrt_specialty_density_hourly"
  "silver_db.int_clinical__specialty_visits"
)

# ==============================
# 3. Sinh SQL maintenance
# ==============================

echo "-- Auto generated Iceberg maintenance SQL" >> "$TEMP_SQL_FILE"
echo "-- Generated at $(date)" >> "$TEMP_SQL_FILE"
echo "" >> "$TEMP_SQL_FILE"

for table in "${RAW_TABLES[@]}"; do
    echo "-- Maintenance for ${table}" >> "$TEMP_SQL_FILE"
    echo "CALL his_catalog.system.expire_snapshots(table => '${table}', older_than => TIMESTAMP '${SNAPSHOT_EXPIRE_DATE}', retain_last => 1);" >> "$TEMP_SQL_FILE"
    echo "CALL his_catalog.system.rewrite_manifests('${table}');" >> "$TEMP_SQL_FILE"
    echo "" >> "$TEMP_SQL_FILE"
done

for table in "${HOT_TABLES[@]}"; do
    echo "-- Aggressive maintenance for hot table ${table}" >> "$TEMP_SQL_FILE"
    echo "CALL his_catalog.system.expire_snapshots(table => '${table}', older_than => TIMESTAMP '${SNAPSHOT_EXPIRE_DATE}', retain_last => 1);" >> "$TEMP_SQL_FILE"
    echo "CALL his_catalog.system.rewrite_manifests('${table}');" >> "$TEMP_SQL_FILE"
    echo "CALL his_catalog.system.remove_orphan_files(table => '${table}', older_than => TIMESTAMP '${ORPHAN_EXPIRE_DATE}');" >> "$TEMP_SQL_FILE"
    echo "" >> "$TEMP_SQL_FILE"
done

echo "=> File SQL maintenance:"
cat "$TEMP_SQL_FILE"

echo "=> Đang chạy Spark SQL maintenance..."

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
  -f "$TEMP_SQL_FILE"

STATUS=$?

rm -f "$TEMP_SQL_FILE"

if [ $STATUS -eq 0 ]; then
    echo "$LOG_PREFIX BẢO TRÌ HOÀN TẤT THÀNH CÔNG ---"
else
    echo "$LOG_PREFIX BẢO TRÌ THẤT BẠI, EXIT CODE = $STATUS ---"
fi

exit $STATUS