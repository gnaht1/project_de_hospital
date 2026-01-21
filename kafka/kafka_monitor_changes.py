import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, current_timestamp

# --- SETUP ENV ---
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# --- CONFIG ---
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
KAFKA_BOOTSTRAP_SERVERS = "172.30.2.37:9092"   # <--- ĐIỀN IP CỦA BẠN
KAFKA_TOPIC_PATTERN = "his.core_his_prod.*"            # Bắt tất cả topic

spark = SparkSession.builder \
    .appName("DebeziumMonitor") \
    .config("spark.jars.packages", KAFKA_PACKAGE) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"--- Bat dau theo doi thay doi tren: {KAFKA_BOOTSTRAP_SERVERS} ---")

# 1. READ STREAM
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribePattern", KAFKA_TOPIC_PATTERN) \
    .option("startingOffsets", "latest") \
    .load() # Dùng 'latest' để chỉ theo dõi thay đổi MỚI PHÁT SINH từ lúc chạy code

# 2. DEBUG: Xem raw payload trước
df_debug = df_raw.select(
    col("topic"),
    col("value").cast("string").alias("raw_value"),
    col("timestamp").alias("kafka_time")
)

# 2.1 EXTRACT DEBEZIUM DATA
# Debezium JSON structure: { "payload": { "op": "...", "after": {...}, "source": {...} } }
# Chúng ta dùng get_json_object để moi thông tin ra mà không cần quan tâm schema từng bảng
df_parsed = df_raw.select(
    col("topic"),
    # Lấy loại thao tác (c, u, d, r)
    get_json_object(col("value").cast("string"), "$.payload.op").alias("operation"),
    # Lấy dữ liệu mới (cho Insert/Update)
    get_json_object(col("value").cast("string"), "$.payload.after").alias("data_after"),
    # Lấy thời gian thay đổi (tuỳ chọn)
    col("timestamp").alias("kafka_time")
)

# 3. FILTER & FORMAT (Tuỳ chọn: Làm đẹp dữ liệu)
# Ví dụ: Dịch mã 'op' sang tiếng Anh cho dễ đọc
from pyspark.sql.functions import when
df_monitor = df_parsed.withColumn("action_type", 
    when(col("operation") == "c", "INSERT")
    .when(col("operation") == "u", "UPDATE")
    .when(col("operation") == "d", "DELETE")
    .when(col("operation") == "r", "READ/SNAPSHOT")
    .otherwise(col("operation"))
)

# Chỉ chọn các cột cần thiết để in ra màn hình
final_output = df_monitor.select("kafka_time", "topic", "action_type", "data_after")

# 4. OUTPUT CONSOLE - DEBUG RAW FIRST
print("=== DEBUG: Xem raw payload ===")
debug_query = df_debug.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .queryName("debug_raw") \
    .start()

# 5. OUTPUT CONSOLE - PARSED DATA
print("=== Parsed Data ===")
query = final_output.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .queryName("parsed_data") \
    .start()

# Chờ cả 2 query
debug_query.awaitTermination()
query.awaitTermination()

