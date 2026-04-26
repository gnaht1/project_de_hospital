#!/bin/bash

# Khai báo môi trường
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
SPARK_SQL="/opt/spark/bin/spark-sql"

echo "--- [$(date)] BAT DAU BAO TRI LAKEHOUSE ---"

echo "1. Dang tam dung main_job.py va giai phong RAM..."
pkill -f main_job.py
pkill -f "Hospital_CDC"
sleep 5
rm -rf /root/metastore_db/db.lck

# --- ĐOẠN THÊM MỚI: Nhờ Linux tính toán ngày giờ của 3 ngày trước ---
EXPIRE_DATE=$(date -d "3 days ago" +"%Y-%m-%d %H:%M:%S")
echo "=> Thoi gian se xoa Snapshot: truoc $EXPIRE_DATE"

TABLES=("db.ct_dich_vu_iceberg" "db.ct_address_iceberg" "db.ct_dv_ky_thuat_iceberg" "db.dm_benh_nhan_iceberg" "db.ct_dot_dieu_tri_iceberg")

for table in "${TABLES[@]}"
do
    echo "------------------------------------------"
    echo "2. Dang don dep bang: $table"
    
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
      --conf spark.driver.memory=1g \
      --conf spark.executor.memory=1g \
      --conf spark.hadoop.javax.jdo.option.ConnectionURL="jdbc:derby:;databaseName=/tmp/maintenance_derby;create=true" \
      -e "
        CALL his_catalog.system.rewrite_data_files(table => '$table', options => map('min-input-files','10'));
        CALL his_catalog.system.rewrite_manifests(table => '$table');
        CALL his_catalog.system.expire_snapshots(table => '$table', older_than => TIMESTAMP '$EXPIRE_DATE');
      "
done

echo "------------------------------------------"
# echo "3. Khoi dong lai main_job.py..."
# nohup python3 /root/hospital_de_project/main_job.py > /var/log/main_job.log 2>&1 &

echo "--- [$(date)] HOAN TAT BAO TRI! ---"