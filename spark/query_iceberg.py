import sys
import os
import config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, max as _max, min as _min

# Setup Path cho PySpark
sys.path.append(os.path.join(config.SPARK_PATH, "python"))
sys.path.append(os.path.join(config.SPARK_PATH, "python", "lib", "py4j-0.10.9.7-src.zip"))

def init_spark():
    """Khởi tạo Spark Session để query Iceberg"""
    print(">>> KHỞI TẠO SPARK SESSION ĐỂ QUERY ICEBERG...")
    
    spark = SparkSession.builder \
        .appName("Iceberg_Query_Analysis") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4") \
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
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def list_all_tables(spark):
    """Liệt kê tất cả các bảng Iceberg trong database"""
    print(f"\n{'='*80}")
    print(f"DANH SÁCH CÁC BẢNG ICEBERG TRONG DATABASE: {config.CATALOG_NAME}.{config.DATABASE_NAME}")
    print(f"{'='*80}")
    
    tables_df = spark.sql(f"SHOW TABLES IN {config.CATALOG_NAME}.{config.DATABASE_NAME}")
    tables_df.show(100, truncate=False)
    
    return tables_df

def get_table_info(spark, table_name):
    """Lấy thông tin chi tiết về một bảng"""
    full_table_name = f"{config.CATALOG_NAME}.{config.DATABASE_NAME}.{table_name}"
    
    print(f"\n{'='*80}")
    print(f"THÔNG TIN BẢNG: {full_table_name}")
    print(f"{'='*80}")
    
    # Schema của bảng
    print("\n[1] SCHEMA:")
    spark.sql(f"DESCRIBE {full_table_name}").show(100, truncate=False)
    
    # Số lượng bản ghi
    print("\n[2] SỐ LƯỢNG BẢN GHI:")
    count_df = spark.sql(f"SELECT COUNT(*) as total_records FROM {full_table_name}")
    count_df.show()
    
    # Xem một vài bản ghi mẫu
    print("\n[3] DỮ LIỆU MẪU (10 bản ghi đầu tiên):")
    sample_df = spark.sql(f"SELECT * FROM {full_table_name} LIMIT 10")
    sample_df.show(10, truncate=False)

def query_dm_khoa(spark):
    """Truy vấn bảng DM_KHOA - Danh mục khoa"""
    table_name = f"{config.CATALOG_NAME}.{config.DATABASE_NAME}.dm_khoa_iceberg"
    
    print(f"\n{'='*80}")
    print(f"PHÂN TÍCH BẢNG: {table_name}")
    print(f"{'='*80}")
    
    # Tổng số khoa
    print("\n[1] Tổng số khoa:")
    spark.sql(f"""
        SELECT 
            COUNT(*) as total_departments,
            COUNT(DISTINCT code_khoa) as unique_codes
        FROM {table_name}
    """).show()
    
    # Danh sách các khoa đang hoạt động
    print("\n[2] Các khoa đang hoạt động:")
    spark.sql(f"""
        SELECT code_khoa, ten, active, created_at
        FROM {table_name}
        WHERE active = true AND deleted = 0
        ORDER BY code_khoa
    """).show(50, truncate=False)

def query_dm_benh_nhan(spark):
    """Truy vấn bảng DM_BENH_NHAN - Danh mục bệnh nhân"""
    table_name = f"{config.CATALOG_NAME}.{config.DATABASE_NAME}.dm_benh_nhan_iceberg"
    
    print(f"\n{'='*80}")
    print(f"PHÂN TÍCH BẢNG: {table_name}")
    print(f"{'='*80}")
    
    # Tổng số bệnh nhân
    print("\n[1] Tổng số bệnh nhân:")
    spark.sql(f"""
        SELECT COUNT(*) as total_patients
        FROM {table_name}
    """).show()
    
    # Bệnh nhân mới nhất
    print("\n[2] 10 bệnh nhân đăng ký gần nhất:")
    spark.sql(f"""
        SELECT ma_nb, ten_nb, ngay_sinh, so_dien_thoai, created_at
        FROM {table_name}
        ORDER BY created_at DESC
        LIMIT 10
    """).show(10, truncate=False)

def query_ct_dot_dieu_tri(spark):
    """Truy vấn bảng CT_DOT_DIEU_TRI - Chi tiết đợt điều trị"""
    table_name = f"{config.CATALOG_NAME}.{config.DATABASE_NAME}.ct_dot_dieu_tri_iceberg"
    
    print(f"\n{'='*80}")
    print(f"PHÂN TÍCH BẢNG: {table_name}")
    print(f"{'='*80}")
    
    # Thống kê theo trạng thái
    print("\n[1] Thống kê theo trạng thái điều trị:")
    spark.sql(f"""
        SELECT 
            trang_thai,
            COUNT(*) as so_luong,
            COUNT(DISTINCT ma_nb) as so_benh_nhan
        FROM {table_name}
        GROUP BY trang_thai
        ORDER BY trang_thai
    """).show()
    
    # Thống kê theo khoa
    print("\n[2] Thống kê theo khoa:")
    spark.sql(f"""
        SELECT 
            khoa_id,
            COUNT(*) as so_dot_dieu_tri,
            COUNT(DISTINCT ma_nb) as so_benh_nhan
        FROM {table_name}
        WHERE khoa_id IS NOT NULL
        GROUP BY khoa_id
        ORDER BY so_dot_dieu_tri DESC
        LIMIT 20
    """).show()
    
    # Thống kê cấp cứu
    print("\n[3] Thống kê ca cấp cứu:")
    spark.sql(f"""
        SELECT 
            cap_cuu,
            COUNT(*) as so_luong
        FROM {table_name}
        GROUP BY cap_cuu
    """).show()

def query_ct_phieu_thu(spark):
    """Truy vấn bảng CT_PHIEU_THU - Chi tiết phiếu thu"""
    table_name = f"{config.CATALOG_NAME}.{config.DATABASE_NAME}.ct_phieu_thu_iceberg"
    
    print(f"\n{'='*80}")
    print(f"PHÂN TÍCH BẢNG: {table_name}")
    print(f"{'='*80}")
    
    # Tổng doanh thu
    print("\n[1] Tổng quan doanh thu:")
    spark.sql(f"""
        SELECT 
            COUNT(*) as tong_phieu_thu,
            SUM(thanh_tien) as tong_thanh_tien,
            SUM(tien_nb_tu_tra) as tong_nb_tu_tra,
            SUM(tien_bh_thanh_toan) as tong_bh_thanh_toan,
            AVG(thanh_tien) as trung_binh_thanh_tien
        FROM {table_name}
        WHERE thanh_toan = 1
    """).show()
    
    # Thống kê theo loại phiếu thu
    print("\n[2] Thống kê theo loại phiếu thu:")
    spark.sql(f"""
        SELECT 
            loai_phieu_thu,
            COUNT(*) as so_luong,
            SUM(thanh_tien) as tong_tien
        FROM {table_name}
        GROUP BY loai_phieu_thu
        ORDER BY tong_tien DESC
    """).show()

def query_dm_dich_vu(spark):
    """Truy vấn bảng DM_DICH_VU - Danh mục dịch vụ"""
    table_name = f"{config.CATALOG_NAME}.{config.DATABASE_NAME}.dm_dich_vu_iceberg"
    
    print(f"\n{'='*80}")
    print(f"PHÂN TÍCH BẢNG: {table_name}")
    print(f"{'='*80}")
    
    # Tổng số dịch vụ
    print("\n[1] Tổng số dịch vụ:")
    spark.sql(f"""
        SELECT 
            COUNT(*) as tong_dich_vu,
            COUNT(DISTINCT loai_dich_vu) as so_loai_dich_vu
        FROM {table_name}
    """).show()
    
    # Thống kê theo loại dịch vụ
    print("\n[2] Thống kê theo loại dịch vụ:")
    spark.sql(f"""
        SELECT 
            loai_dich_vu,
            COUNT(*) as so_luong_dich_vu,
            AVG(gia_khong_bao_hiem) as gia_trung_binh
        FROM {table_name}
        WHERE active = true AND deleted = 0
        GROUP BY loai_dich_vu
        ORDER BY so_luong_dich_vu DESC
    """).show()
    
    # Top 10 dịch vụ đắt nhất
    print("\n[3] Top 10 dịch vụ có giá cao nhất:")
    spark.sql(f"""
        SELECT code_dichvu, ten, gia_khong_bao_hiem, loai_dich_vu
        FROM {table_name}
        WHERE active = true AND deleted = 0 AND gia_khong_bao_hiem IS NOT NULL
        ORDER BY gia_khong_bao_hiem DESC
        LIMIT 10
    """).show(10, truncate=False)

def query_dm_nhan_vien(spark):
    """Truy vấn bảng DM_NHAN_VIEN - Danh mục nhân viên"""
    table_name = f"{config.CATALOG_NAME}.{config.DATABASE_NAME}.dm_nhan_vien_iceberg"
    
    print(f"\n{'='*80}")
    print(f"PHÂN TÍCH BẢNG: {table_name}")
    print(f"{'='*80}")
    
    # Tổng số nhân viên
    print("\n[1] Tổng số nhân viên:")
    spark.sql(f"""
        SELECT 
            COUNT(*) as tong_nhan_vien,
            COUNT(CASE WHEN active = true THEN 1 END) as dang_hoat_dong
        FROM {table_name}
    """).show()
    
    # Thống kê theo học hàm học vị
    print("\n[2] Thống kê theo học hàm học vị:")
    spark.sql(f"""
        SELECT 
            hoc_ham_hoc_vi_id,
            COUNT(*) as so_luong
        FROM {table_name}
        WHERE active = true AND deleted = 0
        GROUP BY hoc_ham_hoc_vi_id
        ORDER BY so_luong DESC
    """).show()

def custom_query(spark, sql_query):
    """Thực thi truy vấn SQL tùy chỉnh"""
    print(f"\n{'='*80}")
    print(f"TRUY VẤN TÙY CHỈNH")
    print(f"{'='*80}")
    print(f"\nSQL: {sql_query}\n")
    
    result_df = spark.sql(sql_query)
    result_df.show(100, truncate=False)
    
    return result_df

def main():
    """Hàm chính để chạy các truy vấn"""
    spark = init_spark()
    
    print("\n" + "="*80)
    print("CHƯƠNG TRÌNH TRUY VẤN 31 BẢNG ICEBERG")
    print("="*80)
    
    # Menu lựa chọn
    while True:
        print("\n" + "="*80)
        print("MENU CHỨC NĂNG:")
        print("="*80)
        print("1.  Liệt kê tất cả các bảng")
        print("2.  Xem thông tin chi tiết một bảng")
        print("3.  Phân tích bảng DM_KHOA (Danh mục khoa)")
        print("4.  Phân tích bảng DM_BENH_NHAN (Danh mục bệnh nhân)")
        print("5.  Phân tích bảng CT_DOT_DIEU_TRI (Đợt điều trị)")
        print("6.  Phân tích bảng CT_PHIEU_THU (Phiếu thu)")
        print("7.  Phân tích bảng DM_DICH_VU (Danh mục dịch vụ)")
        print("8.  Phân tích bảng DM_NHAN_VIEN (Danh mục nhân viên)")
        print("9.  Truy vấn SQL tùy chỉnh")
        print("10. Phân tích tất cả các bảng (tổng quan)")
        print("0.  Thoát")
        print("="*80)
        
        choice = input("\nNhập lựa chọn của bạn: ").strip()
        
        try:
            if choice == "1":
                list_all_tables(spark)
            
            elif choice == "2":
                table_name = input("Nhập tên bảng (ví dụ: dm_khoa_iceberg): ").strip()
                get_table_info(spark, table_name)
            
            elif choice == "3":
                query_dm_khoa(spark)
            
            elif choice == "4":
                query_dm_benh_nhan(spark)
            
            elif choice == "5":
                query_ct_dot_dieu_tri(spark)
            
            elif choice == "6":
                query_ct_phieu_thu(spark)
            
            elif choice == "7":
                query_dm_dich_vu(spark)
            
            elif choice == "8":
                query_dm_nhan_vien(spark)
            
            elif choice == "9":
                print("\nNhập câu truy vấn SQL (ví dụ: SELECT * FROM my_catalog.db.dm_khoa_iceberg LIMIT 10):")
                sql_query = input().strip()
                custom_query(spark, sql_query)
            
            elif choice == "10":
                # Phân tích tổng quan tất cả các bảng
                print("\n" + "="*80)
                print("PHÂN TÍCH TỔNG QUAN TẤT CẢ CÁC BẢNG")
                print("="*80)
                
                tables = [
                    "dm_khoa_iceberg", "dm_loai_dich_vu_iceberg", "ct_address_iceberg",
                    "ct_bo_chi_dinh_iceberg", "ct_dich_vu_iceberg", "ct_dot_dieu_tri_iceberg",
                    "ct_dv_kham_iceberg", "ct_dv_kham_ket_luan_iceberg", "ct_dv_ky_thuat_iceberg",
                    "ct_kham_suc_khoe_iceberg", "ct_nguon_nb_iceberg", "ct_phieu_thu_iceberg",
                    "dm_benh_nhan_iceberg", "dm_bo_chi_dinh_iceberg", "dm_chuyen_khoa_iceberg",
                    "dm_dich_vu_iceberg", "dm_doi_tuong_kcb_iceberg", "dm_dv_discount_iceberg",
                    "dm_hoc_ham_hoc_vi_iceberg", "dm_hop_dong_ksk_iceberg", "dm_nguoi_gioi_thieu_iceberg",
                    "dm_nguon_nb_iceberg", "dm_nhan_vien_iceberg", "dm_nhom_dich_vu_cap1_iceberg",
                    "dm_nhom_dich_vu_cap2_iceberg", "dm_nhom_dich_vu_cap3_iceberg", "dm_phong_iceberg",
                    "dm_quan_huyen_iceberg", "dm_tinh_thanh_pho_iceberg", "dm_xa_phuong_iceberg",
                    "hospital_configs_iceberg"
                ]
                
                for table in tables:
                    full_table_name = f"{config.CATALOG_NAME}.{config.DATABASE_NAME}.{table}"
                    try:
                        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}").collect()[0]['cnt']
                        print(f"{table:40s} : {count:>10,} bản ghi")
                    except Exception as e:
                        print(f"{table:40s} : Lỗi - {str(e)[:50]}")
            
            elif choice == "0":
                print("\nĐang thoát chương trình...")
                break
            
            else:
                print("\n⚠️  Lựa chọn không hợp lệ. Vui lòng thử lại.")
        
        except Exception as e:
            print(f"\n❌ LỖI: {e}")
            import traceback
            traceback.print_exc()
    
    spark.stop()
    print("\n✅ Đã đóng Spark Session. Tạm biệt!")

if __name__ == "__main__":
    main()
