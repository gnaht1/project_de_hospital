import json
from kafka import KafkaConsumer

# --- CẤU HÌNH ---
KAFKA_SERVER = '172.30.2.37:9092' # IP VPS Kafka
TOPIC_NAME = 'his.core_his_prod.dm_khoa' # Chọn 1 topic bất kỳ có data

def main():
    print(f"Đang kết nối tới {TOPIC_NAME}...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='earliest', # Đọc từ đầu để chắc chắn có data
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Đang chờ tin nhắn (nhấn Ctrl+C để thoát)...")
    
    # Chỉ lấy đúng 1 tin nhắn đầu tiên rồi dừng
    for message in consumer:
        print("-" * 50)
        print("CẤU TRÚC JSON CỦA BẠN LÀ:")
        print("-" * 50)
        
        # In ra dạng đẹp (Pretty Print)
        print(json.dumps(message.value, indent=4, ensure_ascii=False))
        
        break # Dừng ngay sau khi bắt được 1 tin

if __name__ == "__main__":
    main()