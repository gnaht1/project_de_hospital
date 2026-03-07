import sys
import os
import config # Import file config.py
import schemas # Import file schemas.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, lit, row_number
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException

# Setup Path cho PySpark (Do file config đã chạy rồi nên ở đây chỉ cần append sys.path)
sys.path.append(os.path.join(config.SPARK_PATH, "python"))
sys.path.append(os.path.join(config.SPARK_PATH, "python", "lib", "py4j-0.10.9.7-src.zip"))

def create_iceberg_table_if_not_exists(spark, full_table_name, schema):
    """Hàm tự động tạo bảng Iceberg dựa trên Schema nếu chưa có"""
    try:
        spark.read.table(full_table_name)
        print(f"  [OK] Bảng {full_table_name} đã tồn tại.")
    except AnalysisException:
        print(f"  [NEW] Bảng {full_table_name} chưa có. Đang tạo mới...")
        try:
            # Chuyển đổi PySpark schema sang DDL string
            from pyspark.sql.types import IntegerType, StringType, BooleanType, TimestampType, DoubleType, LongType, FloatType
            
            def spark_type_to_sql(spark_type):
                """Chuyển đổi Spark DataType sang SQL type string"""
                if isinstance(spark_type, IntegerType):
                    return "INT"
                elif isinstance(spark_type, StringType):
                    return "STRING"
                elif isinstance(spark_type, BooleanType):
                    return "BOOLEAN"
                elif isinstance(spark_type, TimestampType):
                    return "TIMESTAMP"
                elif isinstance(spark_type, DoubleType):
                    return "DOUBLE"
                elif isinstance(spark_type, FloatType):
                    return "FLOAT"
                elif isinstance(spark_type, LongType):
                    return "LONG"
                else:
                    return "STRING"  # Default fallback
            
            # Tạo DDL string từ schema
            fields_ddl = ", ".join([
                f"`{field.name}` {spark_type_to_sql(field.dataType)}"
                for field in schema.fields
            ])
            
            # Tạo bảng bằng SQL DDL (không partition theo op vì dùng MERGE INTO)
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    {fields_ddl}
                )
                USING iceberg
            """
            
            spark.sql(create_table_sql)
            print("  -> Tạo thành công!")
        except Exception as e:
            print(f"  -> Lỗi tạo bảng: {e}")

def start_stream_for_topic(spark, topic, conf):
    """Hàm khởi tạo luồng xử lý cho 1 topic (MERGE INTO / Upsert)"""
    table_name = conf["table_name"]
    raw_schema = conf["schema"]
    primary_keys = conf["primary_keys"]  # Khóa chính để MERGE
    
    # Lấy Schema đầy đủ (bao gồm vỏ Debezium)
    envelope_schema = schemas.get_debezium_envelope(raw_schema)
    
    full_table_name = f"{config.CATALOG_NAME}.{config.DATABASE_NAME}.{table_name}"
    
    print(f"\n>>> [INIT] Khởi tạo stream: {topic} -> {full_table_name}")
    print(f"    Primary keys: {primary_keys}")

    # 1. Đảm bảo bảng đích tồn tại
    # Schema đích = raw_schema + op + ts_ms (thời điểm CDC) + ingestion_timestamp (thời điểm Spark xử lý)
    target_schema = StructType(
        raw_schema.fields + [
            StructField("op", StringType(), True),
            StructField("ts_ms", StringType(), True),
            StructField("ingestion_timestamp", TimestampType(), True)
        ]
    )
    create_iceberg_table_if_not_exists(spark, full_table_name, target_schema)

    # 2. Đọc Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_SERVER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. Parse JSON & Flatten Data
    # Lấy thêm ts_ms từ Debezium envelope (thời điểm CDC xảy ra)
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), envelope_schema).alias("data")) \
        .select("data.after.*", "data.op", "data.ts_ms") \
        .filter("op != 'd'")  # Lọc bản ghi xóa
    
    # 3.1. Thêm cột ingestion_timestamp = thời điểm Spark xử lý bản ghi
    df_parsed = df_parsed.withColumn("ingestion_timestamp", current_timestamp())
    
    # 3.2. Đảm bảo tất cả các field trong schema đều có trong DataFrame
    existing_columns = set(df_parsed.columns)
    target_columns = [field.name for field in target_schema.fields]
    
    for col_name in target_columns:
        if col_name not in existing_columns:
            df_parsed = df_parsed.withColumn(col_name, lit(None).cast(StringType()))
    
    # Chọn đúng thứ tự các cột theo target_schema
    df_parsed = df_parsed.select(*target_columns)
    
    if table_name == "dm_khoa_iceberg":
        print(f">>> ĐANG ÁP DỤNG FILTER CHO BẢNG: {table_name}")
        df_parsed = df_parsed.filter("ten != 'TEST_01'")

    # 4. Ghi xuống Iceberg bằng Upsert (DataFrame-based)
    # Mỗi lần ghi, Iceberg tạo snapshot mới -> "nhảy version" trên MinIO
    # Query bình thường luôn lấy snapshot mới nhất (latest version)
    # Muốn xem data cũ -> dùng Time Travel:
    #   SELECT * FROM table VERSION AS OF <snapshot_id>;
    #   SELECT * FROM table TIMESTAMP AS OF '2026-03-01 10:00:00';
    def upsert_to_iceberg(batch_df, batch_id):
        """Upsert: PK đã tồn tại -> UPDATE, chưa có -> INSERT.
        Iceberg tự động tạo snapshot mới sau mỗi lần ghi."""
        if batch_df.isEmpty():
            print(f"  [SKIP] Batch {batch_id} | Table: {table_name} | Batch rỗng, bỏ qua.")
            return
        
        print(f"  [PROCESSING] Batch {batch_id} | Table: {table_name} | Incoming rows: {batch_df.count()}")
        
        try:
            # Deduplicate trong cùng 1 batch: giữ bản ghi mới nhất theo ts_ms
            window_spec = Window.partitionBy(*primary_keys).orderBy(col("ts_ms").desc())
            new_data = batch_df.withColumn("_row_num", row_number().over(window_spec)) \
                               .filter("_row_num = 1") \
                               .drop("_row_num")
            
            # Đọc dữ liệu hiện tại trong bảng Iceberg
            existing_df = batch_df.sparkSession.read.table(full_table_name)
            
            # Loại bỏ các bản ghi cũ có cùng PK với batch mới (sẽ được thay bằng bản mới)
            # LEFT ANTI JOIN: giữ lại các bản ghi cũ KHÔNG CÓ trong batch mới
            join_condition = [existing_df[pk] == new_data[pk] for pk in primary_keys]
            remaining_old = existing_df.join(new_data, on=join_condition, how="left_anti")
            
            # Union: bản ghi cũ (đã loại trùng) + bản ghi mới
            result_df = remaining_old.unionByName(new_data)
            
            # Ghi đè toàn bộ bảng Iceberg (tạo snapshot mới)
            result_df.write.format("iceberg").mode("overwrite").save(full_table_name)
            
            print(f"  [DONE] Batch {batch_id} | Table: {table_name} | "
                  f"Old: {existing_df.count()}, New: {new_data.count()}, Result: {result_df.count()}")
        
        except Exception as e:
            print(f"  [ERROR] Batch {batch_id} | Table: {table_name} | Lỗi: {e}")
            import traceback
            traceback.print_exc()

    query = df_parsed.writeStream \
        .foreachBatch(upsert_to_iceberg) \
        .trigger(processingTime="30 seconds") \
        .option("checkpointLocation", f"s3a://{config.BUCKET_NAME}/checkpoints/{table_name}") \
        .start()
    
    return query

def main():
    print(">>> KHOI TAO HE THONG MULTI-TABLE STREAMING...")
    
    # Setup Temp Dirs (Tránh lỗi trên Windows)
    temp_dirs = ["tmp/spark", "tmp/hadoop", "tmp/s3a"]
    for d in temp_dirs: os.makedirs(os.path.join(os.getcwd(), d), exist_ok=True)

    # Init Spark
    spark = SparkSession.builder \
        .appName("Hospital_CDC_Master") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", f"s3a://{config.BUCKET_NAME}/iceberg_warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_URL) \
        .config("spark.hadoop.fs.s3a.access.key", config.ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", config.SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.buffer.dir", os.path.join(os.getcwd(), "tmp/s3a")) \
        .config("spark.hadoop.hadoop.tmp.dir", os.path.join(os.getcwd(), "tmp/hadoop")) \
        .config("spark.local.dir", os.path.join(os.getcwd(), "tmp/spark")) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    # Tạo Database nếu chưa có
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.CATALOG_NAME}.{config.DATABASE_NAME}")

    active_streams = []

    # --- VÒNG LẶP KỲ DIỆU ---
    # Lặp qua tất cả bảng trong file schemas.py và chạy
    for topic, conf in schemas.TABLE_CONFIGS.items():
        try:
            stream = start_stream_for_topic(spark, topic, conf)
            active_streams.append(stream)
        except Exception as e:
            print(f"!!! LỖI KHỞI TẠO TOPIC {topic}: {e}")

    print(f"\n>>> ĐÃ KÍCH HOẠT {len(active_streams)} STREAM(S). ĐANG CHẠY...")
    
    # Chờ tất cả các stream (đừng tắt)
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()