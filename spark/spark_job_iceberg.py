import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType
from pyspark.sql.utils import AnalysisException

# --- 1. CẤU HÌNH PATH CHO WINDOWS (BẢN PIP) ---
# Sửa lại đường dẫn này theo máy bạn
SPARK_PATH = r"C:\Users\rexmi\AppData\Local\Programs\Python\Python314\Lib\site-packages\pyspark"

# 1. Định nghĩa đường dẫn gốc (Phải khai báo dòng này trước!)
HADOOP_BASE_DIR = r"C:\hadoop" 

# 2. Set HADOOP_HOME
os.environ['HADOOP_HOME'] = HADOOP_BASE_DIR

# 3. Thêm thư mục BIN vào PATH hệ thống ngay lập tức
bin_path = os.path.join(HADOOP_BASE_DIR, "bin")
if bin_path not in os.environ['PATH']:
    os.environ['PATH'] = os.environ['PATH'] + ";" + bin_path


sys.path.append(os.path.join(SPARK_PATH, "python"))
sys.path.append(os.path.join(SPARK_PATH, "python", "lib", "py4j-0.10.9.7-src.zip"))

# --- 2. CẤU HÌNH KẾT NỐI ---
KAFKA_SERVER = "172.30.2.37:9092"   # IP VPS Kafka
MINIO_URL = "http://172.30.2.254:9000" # IP VPS MinIO (Thay XX bằng IP thật của VPS MinIO)
ACCESS_KEY = "admin"
SECRET_KEY = "12345678"
BUCKET_NAME = "hospital-datalake"

# Topic bạn muốn lấy
TOPIC_NAME = "his.core_his_prod.dm_khoa" 

def main():
    print(">>> Khởi tạo Spark trên Windows...")
    
    # Tạo thư mục temp cho Hadoop/S3A (tránh lỗi DiskErrorException)
    temp_dirs = [
        os.path.join(os.getcwd(), "tmp", "spark"),
        os.path.join(os.getcwd(), "tmp", "hadoop"),
        os.path.join(os.getcwd(), "tmp", "s3a")
    ]
    for temp_dir in temp_dirs:
        os.makedirs(temp_dir, exist_ok=True)
    print(f">>> Đã tạo thư mục temp: {os.path.join(os.getcwd(), 'tmp')}")
    
    spark = SparkSession.builder \
        .appName("DebeziumToIceberg") \
        .master("local[*]") \
        .config("spark.jars.packages", 
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", f"s3a://{BUCKET_NAME}/iceberg_warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.local.dir", os.path.join(os.getcwd(), "tmp", "spark")) \
        .config("spark.hadoop.hadoop.tmp.dir", os.path.join(os.getcwd(), "tmp", "hadoop")) \
        .config("spark.hadoop.fs.s3a.buffer.dir", os.path.join(os.getcwd(), "tmp", "s3a")) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # --- 3. ĐỊNH NGHĨA SCHEMA (QUAN TRỌNG NHẤT) ---
    
    # Schema chi tiết của bảng dm_khoa (nằm trong 'after')
    dm_khoa_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("code_khoa", StringType(), True),
        StructField("ten", StringType(), True),
        StructField("active", BooleanType(), True),  # JSON của bạn là false (boolean), không phải string
        StructField("deleted", IntegerType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])

    # Schema bao bọc bên ngoài (Envelope của Debezium)
    # Dựa trên JSON bạn gửi: { "before": ..., "after": { ... }, "op": ... }
    envelope_schema = StructType([
        StructField("before", StringType(), True), # Không cần lấy chi tiết before
        StructField("after", dm_khoa_schema, True), # Nhúng schema dm_khoa vào đây
        StructField("op", StringType(), True),      # Lấy thêm op để biết là Insert/Update/Delete
        StructField("ts_ms", StringType(), True)
    ])

    print(f">>> Đang đọc Kafka Topic: {TOPIC_NAME}")

    # Đọc Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

    # --- 4. XỬ LÝ DỮ LIỆU ---
    
    # Bước 1: Parse JSON bọc ngoài
    # Lưu ý: Debezium thường gửi JSON dạng {"payload": ...}. 
    # Nếu JSON bạn gửi là ROOT, ta dùng envelope_schema trực tiếp.
    # Nếu JSON bạn gửi nằm trong 'payload', ta cần get_json_object trước.
    
    # Cách an toàn nhất: Dùng get_json_object để móc thẳng vào field "after" bất kể nó nằm sâu bao nhiêu
    # Giả sử cấu trúc JSON bạn gửi là chuẩn
    
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_string") \
        .select(
            # Parse phần 'after' dựa trên schema dm_khoa
            # Nếu JSON thực tế có chữ "payload" bọc ngoài -> sửa thành "$.payload.after"
            # Dựa vào JSON bạn gửi (bắt đầu bằng "before"), path là "$.after"
            from_json(col("json_string"), envelope_schema).alias("envelope")
        ) \
        .select("envelope.after.*", "envelope.op") # Bung lụa các cột ra

    # (Tùy chọn) Lọc bỏ bản ghi Delete (op = 'd') vì field 'after' sẽ là null
    df_clean = df_parsed.filter("op != 'd'")

    print(">>> Schema sau khi parse:")
    df_clean.printSchema()

    # --- 5. GHI VÀO ICEBERG ---
    table_name = "dm_khoa_iceberg"
    full_table_name = f"my_catalog.db.{table_name}"
    
    print(f">>> Kiểm tra bảng Iceberg: {full_table_name}")

    # 1. Tạo Database (Namespace) tên là 'db' nếu chưa có
    spark.sql("CREATE DATABASE IF NOT EXISTS my_catalog.db")

    # 2. Kiểm tra xem bảng có tồn tại chưa
    try:
        # Thử đọc bảng, nếu lỗi nghĩa là chưa có
        spark.read.table(full_table_name)
        print(f">>> Bảng {full_table_name} đã tồn tại. Sẵn sàng ghi.")
    except AnalysisException:
        print(f">>> Bảng {full_table_name} chưa tồn tại. Đang tạo mới...")
        
        # Dùng SQL thuần để tạo bảng (tránh lỗi serialization)
        try:
            spark.sql(f"""
                CREATE TABLE {full_table_name} (
                    id INT,
                    code_khoa STRING,
                    ten STRING,
                    active BOOLEAN,
                    deleted INT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    op STRING
                )
                USING iceberg
                PARTITIONED BY (op)
            """)
            print(">>> Tạo bảng thành công!")
        except Exception as e:
            print(f"Lỗi khi tạo bảng: {e}")
            return  # Dừng chương trình nếu không tạo được bảng
    
    print(f">>> Bắt đầu ghi vào bảng: {table_name}")
    
    query = df_clean.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", f"s3a://{BUCKET_NAME}/checkpoints/{table_name}") \
        .option("path", f"my_catalog.db.{table_name}") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()