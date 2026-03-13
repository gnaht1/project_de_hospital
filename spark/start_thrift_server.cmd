@echo off
REM ============================================================
REM  Khởi chạy Spark Thrift Server (HiveServer2) với Iceberg + MinIO
REM  Port mặc định: 10000 (dbt-spark sẽ kết nối vào đây)
REM ============================================================

REM --- CONFIG (đồng bộ với config.py) ---
set SPARK_HOME=C:\Users\rexmi\AppData\Local\Programs\Python\Python314\Lib\site-packages\pyspark
set HADOOP_HOME=C:\hadoop
set PATH=%HADOOP_HOME%\bin;%PATH%

set MINIO_ENDPOINT=http://172.30.2.254:9000
set MINIO_ACCESS_KEY=admin
set MINIO_SECRET_KEY=12345678
set WAREHOUSE_PATH=s3a://hospital-datalake/iceberg_warehouse
set CATALOG_NAME=my_catalog
set DATABASE_NAME=db

REM --- JAR packages ---
set PACKAGES=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262

echo ============================================================
echo  SPARK THRIFT SERVER - Iceberg + MinIO
echo  Endpoint: %MINIO_ENDPOINT%
echo  Warehouse: %WAREHOUSE_PATH%
echo  Catalog: %CATALOG_NAME%.%DATABASE_NAME%
echo  Thrift Port: 10000
echo ============================================================

REM --- Khởi chạy Spark Thrift Server ---
"%SPARK_HOME%\bin\spark-submit.cmd" ^
  --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 ^
  --name "Hospital_Thrift_Server" ^
  --master "local[*]" ^
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" ^
  --conf "spark.sql.catalog.%CATALOG_NAME%=org.apache.iceberg.spark.SparkCatalog" ^
  --conf "spark.sql.catalog.%CATALOG_NAME%.type=hadoop" ^
  --conf "spark.sql.catalog.%CATALOG_NAME%.warehouse=%WAREHOUSE_PATH%" ^
  --conf "spark.sql.catalog.%CATALOG_NAME%.default-namespace=%DATABASE_NAME%" ^
  --conf "spark.hadoop.fs.s3a.endpoint=%MINIO_ENDPOINT%" ^
  --conf "spark.hadoop.fs.s3a.access.key=%MINIO_ACCESS_KEY%" ^
  --conf "spark.hadoop.fs.s3a.secret.key=%MINIO_SECRET_KEY%" ^
  --conf "spark.hadoop.fs.s3a.path.style.access=true" ^
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" ^
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" ^
  --conf "spark.sql.catalogImplementation=in-memory" ^
  --conf "spark.sql.defaultCatalog=%CATALOG_NAME%" ^
  --conf "spark.sql.warehouse.dir=%WAREHOUSE_PATH%" ^
  --conf "spark.hive.server2.thrift.port=10000" ^
  --conf "spark.hive.server2.thrift.bind.host=0.0.0.0" ^
  --conf "spark.sql.hive.thriftServer.initialDatabase=%CATALOG_NAME%.%DATABASE_NAME%"

echo.
echo Thrift Server đã dừng.
pause
