#!/bin/bash

# ==============================
# Iceberg Lakehouse Maintenance - Light
# Run every 3-6 hours.
# Only expires snapshots for hot dbt/NRT tables.
# ==============================

set -o pipefail

export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
SPARK_SQL="/opt/spark/bin/spark-sql"

LOG_PREFIX="--- [$(date '+%Y-%m-%d %H:%M:%S')]"

echo "$LOG_PREFIX BAT DAU LIGHT MAINTENANCE ---"

SNAPSHOT_EXPIRE_DATE=$(date -d "2 hours ago" +"%Y-%m-%d %H:%M:%S")

echo "=> Se xoa snapshots HOT tables cu hon: $SNAPSHOT_EXPIRE_DATE"

TEMP_SQL_FILE="/tmp/run_iceberg_maintenance_light.sql"
> "$TEMP_SQL_FILE"

HOT_TABLES=(
  "gold_db.mart_core__dm_benh_nhan_snapshot"
  "gold_db.mart_finance__nrt_revenue_receipts"
  "gold_db.mart_finance__nrt_revenue_today_vs_yesterday"
  "gold_db.mart_clinical__nrt_specialty_density_hourly"
  "silver_db.int_clinical__specialty_visits"
)

echo "-- Auto generated Iceberg light maintenance SQL" >> "$TEMP_SQL_FILE"
echo "-- Generated at $(date)" >> "$TEMP_SQL_FILE"
echo "" >> "$TEMP_SQL_FILE"

for table in "${HOT_TABLES[@]}"; do
    echo "-- Expire snapshots for hot table ${table}" >> "$TEMP_SQL_FILE"
    echo "CALL his_catalog.system.expire_snapshots(table => '${table}', older_than => TIMESTAMP '${SNAPSHOT_EXPIRE_DATE}', retain_last => 1);" >> "$TEMP_SQL_FILE"
    echo "" >> "$TEMP_SQL_FILE"
done

echo "=> File SQL maintenance:"
cat "$TEMP_SQL_FILE"

echo "=> Dang chay Spark SQL light maintenance..."

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
  --conf spark.hadoop.javax.jdo.option.ConnectionURL="jdbc:derby:;databaseName=/tmp/maintenance_light_derby;create=true" \
  -f "$TEMP_SQL_FILE"

STATUS=$?

rm -f "$TEMP_SQL_FILE"

if [ $STATUS -eq 0 ]; then
    echo "$LOG_PREFIX LIGHT MAINTENANCE HOAN TAT THANH CONG ---"
else
    echo "$LOG_PREFIX LIGHT MAINTENANCE THAT BAI, EXIT CODE = $STATUS ---"
fi

exit $STATUS
