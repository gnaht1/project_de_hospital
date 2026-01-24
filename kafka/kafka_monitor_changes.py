import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, when

# ==========================================
# 1. CẤU HÌNH MÔI TRƯỜNG WINDOWS (FIX CỨNG)
# ==========================================

# ⚠️ QUAN TRỌNG: Hãy sửa đường dẫn này trỏ tới thư mục chứa folder 'bin' của bạn
# Ví dụ: Nếu bạn để winutils.exe tại C:\hadoop\bin\winutils.exe thì điền: C:\\hadoop
HADOOP_HOME_PATH = "C:\\hadoop" 

# Setup biến môi trường ngay trong code
os.environ['HADOOP_HOME'] = HADOOP_HOME_PATH
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Thêm folder bin vào PATH của hệ thống (chỉ trong phiên chạy này)
if os.path.join(HADOOP_HOME_PATH, 'bin') not in os.environ['PATH']:
    print(f"--- Dang them Hadoop bin vao Path: {HADOOP_HOME_PATH}\\bin ---")
    os.environ['PATH'] += os.pathsep + os.path.join(HADOOP_HOME_PATH, 'bin')

# Kiểm tra file hadoop.dll có tồn tại không
hadoop_dll = os.path.join(HADOOP_HOME_PATH, 'bin', 'hadoop.dll')
if not os.path.exists(hadoop_dll):
    print(f"❌ LOI: Khong tim thay file {hadoop_dll}")
    print("   Vui long tai hadoop.dll va winutils.exe bo vao thu muc bin!")
    sys.exit(1) # Dừng chương trình luôn nếu thiếu file

# ==========================================
# 2. CẤU HÌNH SPARK & KAFKA
# ==========================================
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
KAFKA_BOOTSTRAP_SERVERS = "172.30.2.37:9092"
KAFKA_TOPIC_PATTERN = "his.core_his_prod.*" 

spark = SparkSession.builder \
    .appName("DebeziumMonitorFinal") \
    .config("spark.jars.packages", KAFKA_PACKAGE) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f"--- MONITOR STARTED | SERVER: {KAFKA_BOOTSTRAP_SERVERS} ---")

# ==========================================
# 3. XỬ LÝ DỮ LIỆU
# ==========================================

# Đọc dữ liệu từ Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribePattern", KAFKA_TOPIC_PATTERN) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON (Đã sửa theo cấu trúc Flattened của bạn)
# Cấu trúc: {"op": "u", "after": {...}, ...} -> Không có payload
df_parsed = df_raw.select(
    col("topic"),
    # Sửa: Dùng $.op thay vì $.payload.op
    get_json_object(col("value").cast("string"), "$.op").alias("operation"),
    # Sửa: Dùng $.after thay vì $.payload.after
    get_json_object(col("value").cast("string"), "$.after").alias("data_after"),
    col("timestamp").alias("kafka_time")
)

# Chuyển mã op sang tiếng Anh dễ đọc
df_monitor = df_parsed.withColumn("action_type", 
    when(col("operation") == "c", "INSERT")
    .when(col("operation") == "u", "UPDATE")
    .when(col("operation") == "d", "DELETE")
    .when(col("operation") == "r", "READ/SNAPSHOT")
    .otherwise(col("operation")) # Giữ nguyên nếu lạ
)

# Chọn cột hiển thị cuối cùng
final_output = df_monitor.select("kafka_time", "topic", "action_type", "data_after")

# ==========================================
# 4. HIỂN THỊ RA MÀN HÌNH
# ==========================================
print("--- Dang cho du lieu thay doi tu Database... ---")

query = final_output.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()