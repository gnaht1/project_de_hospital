@echo off
REM ============================================================
REM  Khởi chạy Spark SQL Terminal để khởi tạo Database
REM ============================================================

set SPARK_HOME=C:\Users\rexmi\AppData\Local\Programs\Python\Python314\Lib\site-packages\pyspark
set HADOOP_HOME=C:\hadoop
set PATH=%HADOOP_HOME%\bin;%PATH%

set MINIO_ENDPOINT=http://172.30.2.254:9000
set MINIO_ACCESS_KEY=admin
set MINIO_SECRET_KEY=12345678
set WAREHOUSE_PATH=s3a://hospital-datalake/iceberg_warehouse
set CATALOG_NAME=my_catalog

set PACKAGES=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262

echo ============================================================
echo  Dang khoi dong Spark SQL Terminal...
echo ============================================================

"%SPARK_HOME%\bin\spark-sql.cmd" ^
  --packages %PACKAGES% ^
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" ^
  --conf "spark.sql.catalog.%CATALOG_NAME%=org.apache.iceberg.spark.SparkCatalog" ^
  --conf "spark.sql.catalog.%CATALOG_NAME%.type=hadoop" ^
  --conf "spark.sql.catalog.%CATALOG_NAME%.warehouse=%WAREHOUSE_PATH%" ^
  --conf "spark.hadoop.fs.s3a.endpoint=%MINIO_ENDPOINT%" ^
  --conf "spark.hadoop.fs.s3a.access.key=%MINIO_ACCESS_KEY%" ^
  --conf "spark.hadoop.fs.s3a.secret.key=%MINIO_SECRET_KEY%" ^
  --conf "spark.hadoop.fs.s3a.path.style.access=true" ^
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" ^
  --conf "spark.sql.defaultCatalog=%CATALOG_NAME%"