from pyspark.sql.types import VarcharType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, DoubleType, FloatType

# Function to automatically wrap Schema into Debezium envelope
def get_debezium_envelope(table_schema):
    return StructType([
        StructField("before", StringType(), True),
        StructField("after", table_schema, True), # Data really here
        StructField("op", StringType(), True),
        StructField("ts_ms", StringType(), True) # Change to LongType if needed
    ])

# ====================================================
# DEFINE TABLES (Add 31 tables here)
# ====================================================

# Table 1: DM_KHOA
schema_dm_khoa = StructType([
    StructField("id", IntegerType(), True),
    StructField("code_khoa", StringType(), True),
    StructField("ten", StringType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 2: DM_LOAI_DICH_VU
schema_dm_loai_dich_vu = StructType([
    StructField("id", IntegerType(), True),
    StructField("ten", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 3: CT_ADDRESS
schema_ct_address = StructType([
    StructField("nb_dot_dieu_tri_id", IntegerType(), True),
    StructField("so_nha", StringType(), True),
    StructField("so_nha_tam_tru", StringType(), True),
    StructField("xa_phuong_id", FloatType(), True),
    StructField("xa_phuong_tam_tru_id", StringType(), True),
    StructField("quan_huyen_id", FloatType(), True),
    StructField("quan_huyen_tam_tru_id", StringType(), True),
    StructField("tinh_thanh_pho_id", FloatType(), True),
    StructField("tinh_thanh_pho_tam_tru_id", StringType(), True),
    StructField("dia_chi_cong_ty", StringType(), True),
    StructField("ten_cong_ty", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
])

# Table 4: CT_BO_CHI_DINH
schema_ct_bo_chi_dinh = StructType([
    StructField("id", IntegerType(), True),
    StructField("nb_dot_dieu_tri_id", IntegerType(), True),
    StructField("bo_chi_dinh_id", IntegerType(), True),
    StructField("thoi_gian_chi_dinh", StringType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 5: CT_DICH_VU
# Table 5: CT_DICH_VU
schema_ct_dich_vu = StructType([
    StructField("id", IntegerType(), True),
    StructField("nb_dot_dieu_tri_id", IntegerType(), True),
    StructField("bac_si_chi_dinh_id", IntegerType(), True),
    StructField("chi_dinh_tu_dich_vu_id", IntegerType(), True),
    StructField("chi_dinh_tu_loai_dich_vu", IntegerType(), True),
    StructField("dich_vu_id", IntegerType(), True),
    StructField("doi_tuong_kcb", IntegerType(), True),
    StructField("dung_tuyen", IntegerType(), True),
    StructField("ghi_chu", StringType(), True),
    StructField("gia_bao_hiem", DoubleType(), True),
    StructField("gia_goc", DoubleType(), True),
    StructField("gia_khong_bao_hiem", DoubleType(), True),
    StructField("gia_phu_thu", DoubleType(), True),
    StructField("khoa_chi_dinh_id", IntegerType(), True),
    StructField("khong_thu_tien", BooleanType(), True),
    StructField("khong_tinh_tien", BooleanType(), True),
    StructField("loai_dich_vu", IntegerType(), True),
    StructField("loai_doi_tuong_id", FloatType(), True),
    StructField("loai_hinh_thanh_toan_id", StringType(), True),
    StructField("mien_cung_chi_tra", BooleanType(), True),
    StructField("muc_huong", StringType(), True),
    StructField("nb_bo_chi_dinh_id", FloatType(), True),
    StructField("nb_chuyen_khoa_id", StringType(), True),
    StructField("nb_goi_dv_chi_tiet_id", StringType(), True),
    StructField("nb_goi_pt_tt_id", StringType(), True),
    StructField("nb_the_bao_hiem_id", StringType(), True),
    StructField("ngoai_vien", BooleanType(), True),
    StructField("phan_tram_mien_giam_dich_vu_bh", FloatType(), True),
    StructField("phan_tram_mien_giam_dich_vu_khong_bh", FloatType(), True),
    StructField("phat_hanh_hoa_don", BooleanType(), True),
    StructField("phieu_doi_tra_id", StringType(), True),
    StructField("phieu_thu_id", FloatType(), True),
    StructField("so_luong", FloatType(), True),
    StructField("stt_hien_thi", StringType(), True),
    StructField("thanh_toan", IntegerType(), True), # Để Integer phòng trường hợp là status code (0,1,2)
    StructField("thoi_gian_chi_dinh", StringType(), True),
    StructField("thoi_gian_thuc_hien", StringType(), True),
    StructField("tien_bh_thanh_toan", DoubleType(), True),
    StructField("tien_giam_gia_bh", DoubleType(), True),
    StructField("tien_giam_gia_khong_bh", DoubleType(), True),
    StructField("tien_mien_giam_dich_vu_bh", DoubleType(), True),
    StructField("tien_mien_giam_dich_vu_khong_bh", DoubleType(), True),
    StructField("tien_mien_giam_dich_vu_nhap_vao", DoubleType(), True),
    StructField("tien_mien_giam_phieu_thu_bh", DoubleType(), True),
    StructField("tien_mien_giam_phieu_thu_khong_bh", DoubleType(), True),
    StructField("tien_nb_cung_chi_tra", DoubleType(), True),
    StructField("tien_nb_phu_thu", DoubleType(), True),
    StructField("tien_nb_trai_tuyen", DoubleType(), True),
    StructField("tien_nb_tu_tra", DoubleType(), True),
    StructField("tien_nguon_khac", DoubleType(), True),
    StructField("trang_thai_hoan", IntegerType(), True),
    StructField("tu_tra", BooleanType(), True),
    StructField("ty_le_bh_tt", FloatType(), True),
    StructField("ty_le_tt_dv", FloatType(), True),
    StructField("dv_gia_id", FloatType(), True),
    StructField("nb_phac_do_dieu_tri_id", StringType(), True),
    StructField("phac_do_dieu_tri_dich_vu_id", StringType(), True),
    StructField("ngoai_vien_id", StringType(), True),
    StructField("nguon_khac_id", StringType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])



# Table 6: CT_DOT_DIEU_TRI
schema_ct_dot_dieu_tri = StructType([
    StructField("id", IntegerType(), True),
    StructField("cap_cuu", BooleanType(), True),
    StructField("chi_nam_sinh", BooleanType(), True),
    StructField("dan_toc_id", FloatType(), True),
    StructField("doi_tuong", IntegerType(), True),
    StructField("doi_tuong_kcb", IntegerType(), True),
    StructField("gioi_tinh", IntegerType(), True),
    StructField("kham_suc_khoe", BooleanType(), True),
    StructField("khoa_id", IntegerType(), True),
    StructField("khoa_tiep_don_id", IntegerType(), True),
    StructField("loai_benh_an_id", StringType(), True),
    StructField("loai_doi_tuong_id", FloatType(), True),
    StructField("ma_benh_an", StringType(), True),
    StructField("ma_ho_so", IntegerType(), True),
    StructField("ma_nb", IntegerType(), True),
    StructField("mac_dinh", BooleanType(), True),
    StructField("nb_thong_tin_id", IntegerType(), True),
    StructField("ngay_sinh", StringType(), True),
    StructField("nghe_nghiep_id", StringType(), True),
    StructField("ngoai_vien", BooleanType(), True),
    StructField("nguoi_lap_benh_an_id", StringType(), True),
    StructField("nhom_mau", StringType(), True),
    StructField("noi_lam_viec", StringType(), True),
    StructField("phan_loai_nb_id", StringType(), True),
    StructField("quoc_tich_id", IntegerType(), True),
    StructField("so_bao_hiem_xa_hoi", StringType(), True),
    StructField("so_dien_thoai", StringType(), True),
    StructField("so_ngay_dieu_tri", IntegerType(), True),
    StructField("so_phoi", StringType(), True),
    StructField("ten_nb", StringType(), True),
    StructField("ten_nb_khong_dau", StringType(), True),
    StructField("thoi_gian_lap_benh_an", StringType(), True),
    StructField("thoi_gian_ra_vien", StringType(), True),
    StructField("thoi_gian_vao_vien", StringType(), True),
    StructField("tiem_chung", BooleanType(), True),
    StructField("trang_thai", IntegerType(), True),
    StructField("uu_tien", BooleanType(), True),
    StructField("duyet_chi_phi", StringType(), True),
    StructField("bang_lai_xe_id", StringType(), True),
    StructField("ma_doi_tuong_kcb_id", StringType(), True),
    StructField("nhan_vien_kinh_doanh_id", StringType(), True),
    StructField("can_nang_vao_vien", StringType(), True),
    StructField("cong_ty_bao_hiem_id", StringType(), True),
    StructField("phan_loai_doi_tuong", FloatType(), True),
    StructField("nguoi_duyet_chi_phi_id", StringType(), True),
    StructField("nguoi_gui_duyet_chi_phi_id", StringType(), True),
    StructField("nguoi_tu_choi_duyet_chi_phi_id", StringType(), True),
    StructField("loai_lien_ket", StringType(), True),
    StructField("nb_lien_ket_id", StringType(), True),
    StructField("ho_ngheo", BooleanType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
]
)

# Table 7: CT_DV_KHAM
schema_ct_dv_kham = StructType([
    StructField("id", IntegerType(), True),
    StructField("nb_dot_dieu_tri_id", IntegerType(), True),
    StructField("bac_si_ket_luan_id", FloatType(), True),
    StructField("bac_si_kham_id", FloatType(), True),
    StructField("dot_kham_moi", BooleanType(), True),
    StructField("nguoi_phien_dich_id", StringType(), True),
    StructField("stt_chuyen_khoa", StringType(), True),
    StructField("thiet_lap", StringType(), True),
    StructField("thoi_gian_kham", StringType(), True),
    StructField("thoi_gian_ket_luan", StringType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 8: CT_DV_KHAM_KET_LUAN
schema_ct_dv_kham_ket_luan = StructType([
    StructField("id", IntegerType(), True),
    StructField("nb_dot_dieu_tri_id", IntegerType(), True),
    StructField("huong_dieu_tri", FloatType(), True),
    StructField("ket_qua_dieu_tri", FloatType(), True),
    StructField("loi_dan", StringType(), True),
    StructField("phong_hen_kham_id", FloatType(), True),
    StructField("so_ngay_cho_don", FloatType(), True),
    StructField("thoi_gian_hen_tai_kham", StringType(), True),
    StructField("thoi_gian_ket_luan", StringType(), True),
    StructField("so_hen_kham", StringType(), True),
    StructField("thong_tin_theo_doi", StringType(), True),
    StructField("den_ngay", StringType(), True),
    StructField("tu_ngay", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 9: CT_DV_KY_THUAT
schema_ct_dv_ky_thuat = StructType([
    StructField("id", IntegerType(), True),
    StructField("nb_dot_dieu_tri_id", IntegerType(), True),
    StructField("cap_cuu", BooleanType(), True),
    StructField("hinh_thuc_tt_ksk", FloatType(), True),
    StructField("in_phieu_chi_dinh", IntegerType(), True),
    StructField("ngoai_vien_id", StringType(), True),
    StructField("phieu_in_id", FloatType(), True),
    StructField("phong_thuc_hien_id", FloatType(), True),
    StructField("so_lan_goi", FloatType(), True),
    StructField("so_phieu_id", FloatType(), True),
    StructField("stt", FloatType(), True),
    StructField("tam_ung", BooleanType(), True),
    StructField("thanh_toan_sau", BooleanType(), True),
    StructField("theo_yeu_cau", BooleanType(), True),
    StructField("thoi_gian_lay_so", StringType(), True),
    StructField("thoi_gian_tiep_nhan", StringType(), True),
    StructField("thuc_hien_tai_khoa", BooleanType(), True),
    StructField("trang_thai", IntegerType(), True),
    StructField("tu_van_vien_id", StringType(), True),
    StructField("uu_tien", BooleanType(), True),
    StructField("ly_do_khong_thuc_hien", StringType(), True),
    StructField("thoi_gian_xac_nhan_khong_thuc_hien", StringType(), True),
    StructField("khong_thuc_hien", BooleanType(), True),
    StructField("ly_doi_khong_thuc_hien", StringType(), True),
    StructField("trang_thai_thong_bao", StringType(), True),
    StructField("thoi_gian_bat_dau", StringType(), True),
    StructField("thoi_gian_hoan_thanh", StringType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 10: CT_KHAM_SUC_KHOE
schema_ct_kham_suc_khoe = StructType([
    StructField("id", IntegerType(), True),
    StructField("chuc_vu", StringType(), True),
    StructField("den_thoi_gian_kham", StringType(), True),
    StructField("den_thoi_gian_lay_mau", StringType(), True),
    StructField("dia_diem_kham", StringType(), True),
    StructField("dia_diem_lay_mau", StringType(), True),
    StructField("ds_bo_chi_dinh_id", StringType(), True),
    StructField("ds_dich_vu_id", StringType(), True),
    StructField("hinh_thuc_tt_dv_ngoai_hd", FloatType(), True),
    StructField("hop_dong_ksk_id", IntegerType(), True),
    StructField("ma_nhan_vien", StringType(), True),
    StructField("ngoai_vien", BooleanType(), True),
    StructField("phong_ban", StringType(), True),
    StructField("stt", IntegerType(), True),
    StructField("thoi_gian_hoan_thanh", StringType(), True),
    StructField("trang_thai", IntegerType(), True),
    StructField("tu_thoi_gian_kham", StringType(), True),
    StructField("tu_thoi_gian_lay_mau", StringType(), True),
    StructField("hinh_thuc_tt_dv_trong_hd", FloatType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 11: CT_NGUON_NB
schema_ct_nguon_nb = StructType([
    StructField("nb_dot_dieu_tri_id", IntegerType(), True),
    StructField("ghi_chu", StringType(), True),
    StructField("nguoi_gioi_thieu_id", FloatType(), True),
    StructField("nguon_nb_id", FloatType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 12: CT_PHIEU_THU
schema_ct_phieu_thu = StructType([
    StructField("id", IntegerType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("nb_dot_dieu_tri_id", IntegerType(), True),
    StructField("ca_lam_viec_id", FloatType(), True),
    StructField("doi_tuong_kcb", IntegerType(), True),
    StructField("ds_ma_giam_gia_id", StringType(), True),
    StructField("ghi_chu", StringType(), True),
    StructField("hinh_thuc_mien_giam", FloatType(), True),
    StructField("hoa_don_id", StringType(), True),
    StructField("ky_hieu", StringType(), True),
    StructField("loai_phieu_thu", IntegerType(), True),
    StructField("nb_goi_dv_id", StringType(), True),
    StructField("nha_thu_ngan_id", FloatType(), True),
    StructField("nho_hon_muc_cung_chi_tra", BooleanType(), True),
    StructField("phan_tram_mien_giam", FloatType(), True),
    StructField("quay_id", FloatType(), True),
    StructField("so_phieu", IntegerType(), True),
    StructField("thanh_tien", FloatType(), True),
    StructField("thanh_toan", IntegerType(), True),
    StructField("thoi_gian_huy_thanh_toan", TimestampType(), True),
    StructField("thoi_gian_thanh_toan", StringType(), True),
    StructField("thu_ngan_huy_thanh_toan_id", FloatType(), True),
    StructField("thu_ngan_id", FloatType(), True),
    StructField("tien_bh_thanh_toan", FloatType(), True),
    StructField("tien_bh_thanh_toan_trong_goi", FloatType(), True),
    StructField("tien_giam_gia", FloatType(), True),
    StructField("tien_hoan_tra", FloatType(), True),
    StructField("tien_mien_giam_dich_vu", FloatType(), True),
    StructField("tien_mien_giam_phieu_thu", FloatType(), True),
    StructField("tien_mien_giam_phieu_thu_nhap_vao", FloatType(), True),
    StructField("tien_nb_cung_chi_tra", FloatType(), True),
    StructField("tien_nb_cung_chi_tra_trong_goi", FloatType(), True),
    StructField("tien_nb_phu_thu", FloatType(), True),
    StructField("tien_nb_tu_tra", FloatType(), True),
    StructField("tien_nguon_khac", FloatType(), True),
    StructField("tien_tai_tro_bao_hiem", FloatType(), True),
    StructField("tien_tai_tro_khong_bao_hiem", FloatType(), True),
    StructField("trang_thai_hoa_don", IntegerType(), True),
    StructField("hoan_ung", BooleanType(), True),
    StructField("loai_mien_giam", StringType(), True),
    StructField("phieu_doi_tra_id", StringType(), True),
    StructField("thoi_gian_tao_phieu", StringType(), True),
    StructField("thoi_gian_cap_nhat_phieu", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 13: DM_BENH_NHAN
schema_dm_benh_nhan = StructType([
    StructField("nb_thong_tin_id", IntegerType(), True),
    StructField("ma_nb", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("ngay_sinh", StringType(), True),
    StructField("noi_lam_viec", StringType(), True),
    StructField("so_dien_thoai", StringType(), True),
    StructField("ten_nb", StringType(), True),
    StructField("ten_nb_khong_dau", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 14: DM_BO_CHI_DINH
schema_dm_bo_chi_dinh = StructType([
    StructField("id", IntegerType(), True),
    StructField("code_bo_chi_dinh", StringType(), True),
    StructField("ten", StringType(), True),
    StructField("ds_bac_si_chi_dinh_id", StringType(), True),
    StructField("ds_doi_tuong_su_dung", StringType(), True),
    StructField("ds_kho_id", StringType(), True),
    StructField("ds_loai_dich_vu", StringType(), True),
    StructField("han_che_khoa_chi_dinh", BooleanType(), True),
    StructField("hop_dong_ksk_id", FloatType(), True),
    StructField("thuoc_chi_dinh_ngoai", BooleanType(), True),
    StructField("ket_qua_lau", BooleanType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 15: DM_CHUYEN_KHOA
schema_dm_chuyen_khoa = StructType([
    StructField("id", IntegerType(), True),
    StructField("ten", StringType(), True),
    StructField("code_chuyen_khoa", StringType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 16: DM_DICH_VU
schema_dm_dich_vu = StructType([
    StructField("id", IntegerType(), True),
    StructField("code_dichvu", StringType(), True),
    StructField("ten", StringType(), True),
    StructField("don_vi_tinh_id", FloatType(), True),
    StructField("ds_nguon_khac_chi_tra", StringType(), True),
    StructField("gia_bao_hiem", StringType(), True),
    StructField("gia_khong_bao_hiem", FloatType(), True),
    StructField("khong_tinh_tien", FloatType(), True),
    StructField("loai_dich_vu", IntegerType(), True),
    StructField("nhom_dich_vu_cap1_id", IntegerType(), True),
    StructField("nhom_dich_vu_cap2_id", FloatType(), True),
    StructField("nhom_dich_vu_cap3_id", FloatType(), True),
    StructField("ten_tuong_duong", StringType(), True),
    StructField("thu_ngoai", BooleanType(), True),
    StructField("ty_le_bh_tt", IntegerType(), True),
    StructField("ty_le_tt_dv", IntegerType(), True),
    StructField("viet_tat", StringType(), True),
    StructField("chi_dinh_sl_le", BooleanType(), True),
    StructField("nguon_khac_id", StringType(), True),
    StructField("gui_vitimes", BooleanType(), True),
    StructField("mien_phi_giam_doc_duyet", BooleanType(), True),
    StructField("khong_su_dung", BooleanType(), True),
    StructField("online_", BooleanType(), True),
    StructField("sua_gia", BooleanType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 17: DM_DOI_TUONG_KCB
schema_dm_doi_tuong_kcb = StructType([
    StructField("id", IntegerType(), True),
    StructField("ten", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 18: DM_DV_DISCOUNT
schema_dm_dv_discount = StructType([
    StructField("id", IntegerType(), True),
    StructField("discount_percent", FloatType(), True),
    StructField("loai_dich_vu_id", StringType(), True),
    StructField("type_discount", IntegerType(), True),
    StructField("fix_amount", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 19: DM_HOC_HAM_HOC_VI
schema_dm_hoc_ham_hoc_vi = StructType([
    StructField("id", IntegerType(), True),
    StructField("code_hoc_ham", StringType(), True),
    StructField("ten", StringType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 20: DM_HOP_DONG_KSK
schema_dm_hop_dong_ksk = StructType([
    StructField("id", IntegerType(), True),
    StructField("code_hop_dong", IntegerType(), True),
    StructField("ten", StringType(), True),
    StructField("ds_ma_giam_gia_id", StringType(), True),
    StructField("hinh_thuc_mien_giam", FloatType(), True),
    StructField("hinh_thuc_tt_dv_ngoai_hd", FloatType(), True),
    StructField("ngay_hieu_luc", StringType(), True),
    StructField("phan_tram_mien_giam", FloatType(), True),
    StructField("so_hop_dong", StringType(), True),
    StructField("thoi_gian_thanh_ly", StringType(), True),
    StructField("tien_chua_thanh_toan", FloatType(), True),
    StructField("tien_da_thanh_toan", FloatType(), True),
    StructField("tien_du_kien", FloatType(), True),
    StructField("tien_du_kien_sau_giam", FloatType(), True),
    StructField("tien_giam_gia", FloatType(), True),
    StructField("tien_mien_giam_dich_vu", FloatType(), True),
    StructField("tien_mien_giam_hop_dong", FloatType(), True),
    StructField("tien_thuc_te", FloatType(), True),
    StructField("tien_thuc_te_sau_giam", FloatType(), True),
    StructField("trang_thai", IntegerType(), True),
    StructField("chot_thanh_toan_dv_ksk", FloatType(), True),
    StructField("tien_nb_da_thanh_toan", FloatType(), True),
    StructField("tien_tai_tro_nb", FloatType(), True),
    StructField("nguoi_gioi_thieu_id", FloatType(), True),
    StructField("nguon_nb_id", FloatType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 21: DM_NGUOI_GIOI_THIEU
schema_dm_nguoi_gioi_thieu = StructType([
    StructField("id", IntegerType(), True),
    StructField("code_nguoi_gioi_thieu", StringType(), True),
    StructField("ten", StringType(), True),
    StructField("ds_nguon_nb_id", StringType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 22: DM_NGUON_NB
schema_dm_nguon_nb = StructType([
    StructField("id", IntegerType(), True),
    StructField("code_nguon_nb", StringType(), True),
    StructField("ten", StringType(), True),
    StructField("nguoi_gioi_thieu", BooleanType(), True),
    StructField("nhom_nguon", IntegerType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 23: DM_NHAN_VIEN
schema_dm_nhan_vien = StructType([
    StructField("id", IntegerType(), True),
    StructField("code_nhan_vien", StringType(), True),
    StructField("ten", StringType(), True),
    StructField("chung_chi", StringType(), True),
    StructField("ds_chuyen_khoa_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gioi_tinh", StringType(), True),
    StructField("hoc_ham_hoc_vi_id", IntegerType(), True),
    StructField("ngay_sinh", StringType(), True),
    StructField("van_bang_id", IntegerType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 24: DM_NHOM_DICH_VU_CAP1
schema_dm_nhom_dich_vu_cap1 = StructType([
    StructField("id", IntegerType(), True),
    StructField("code_service_lvl_1", StringType(), True),
    StructField("ten", StringType(), True),
    StructField("loai_dich_vu", FloatType(), True),
    StructField("stt_bang_ke", IntegerType(), True),
    StructField("trang_thai_hoan_thanh", FloatType(), True),
    StructField("trang_thai_lay_stt", FloatType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 25: DM_NHOM_DICH_VU_CAP2
schema_dm_nhom_dich_vu_cap2 = StructType([
    StructField("id", IntegerType(), True),
    StructField("code_service_lvl_2", StringType(), True),
    StructField("ten", StringType(), True),
    StructField("luu_phim_chup", BooleanType(), True),
    StructField("nhom_dich_vu_cap1_id", IntegerType(), True),
    StructField("phieu_chi_dinh_id", FloatType(), True),
    StructField("tach_stt_noi_tru", BooleanType(), True),
    StructField("tach_stt_uu_tien", BooleanType(), True),
    StructField("theo_yeu_cau", BooleanType(), True),
    StructField("tiep_don_cls", BooleanType(), True),
    StructField("trang_thai_hoan_thanh", FloatType(), True),
    StructField("trang_thai_lay_stt", FloatType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 26: DM_NHOM_DICH_VU_CAP3
schema_dm_nhom_dich_vu_cap3 = StructType([
    StructField("id", IntegerType(), True),
    StructField("code_service_lvl_3", StringType(), True),
    StructField("ten", StringType(), True),
    StructField("nhom_dich_vu_cap2_id", IntegerType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 27: DM_PHONG
schema_dm_phong = StructType([
    StructField("id", IntegerType(), True),
    StructField("code_phong", StringType(), True),
    StructField("ten", StringType(), True),
    StructField("chuyen_khoa_id", FloatType(), True),
    StructField("dia_diem", StringType(), True),
    StructField("ds_loai_phong", StringType(), True),
    StructField("khoa_id", IntegerType(), True),
    StructField("ngoai_tru", BooleanType(), True),
    StructField("ngoai_vien", BooleanType(), True),
    StructField("noi_tru", BooleanType(), True),
    StructField("online", BooleanType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 28: DM_QUAN_HUYEN
schema_dm_quan_huyen = StructType([
    StructField("id", IntegerType(), True),
    StructField("ten", StringType(), True),
    StructField("code_quan_huyen", IntegerType(), True),
    StructField("ma_tcqg", IntegerType(), True),
    StructField("tinh_thanh_pho_id", IntegerType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 39: DM_TINH_THANH_PHO
schema_dm_tinh_thanh_pho = StructType([
    StructField("id", IntegerType(), True),
    StructField("code_province", IntegerType(), True),
    StructField("ten", StringType(), True),
    StructField("ma_tcqg", IntegerType(), True),
    StructField("active", BooleanType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Table 30: DM_XA_PHUONG
schema_dm_xa_phuong = StructType([
    StructField("xa_phuong_id", IntegerType(), True),
    StructField("ten_xa_phuong", StringType(), True),
    StructField("quan_huyen_id", IntegerType(), True),
])

# Table 31: HOSPITAL_CONFIGS
schema_hospital_configs = StructType([
    StructField("id", IntegerType(), True),
    StructField("table_name", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("value_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])


# --- CONFIG TABLES (IMPORTANT) ---
# Key: Kafka Topic Name
# Value: (Iceberg Table Name, Schema, Primary Keys cho MERGE INTO)
TABLE_CONFIGS = {
    "his.core_his_prod.dm_khoa": {
        "table_name": "dm_khoa_iceberg",
        "schema": schema_dm_khoa,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_loai_dich_vu": {
        "table_name": "dm_loai_dich_vu_iceberg",
        "schema": schema_dm_loai_dich_vu,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.ct_address": {
        "table_name": "ct_address_iceberg",
        "schema": schema_ct_address,
        "primary_keys": ["nb_dot_dieu_tri_id"]
    },
    "his.core_his_prod.ct_bo_chi_dinh": {
        "table_name": "ct_bo_chi_dinh_iceberg",
        "schema": schema_ct_bo_chi_dinh,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.ct_dich_vu": {
        "table_name": "ct_dich_vu_iceberg",
        "schema": schema_ct_dich_vu,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.ct_dot_dieu_tri": {
        "table_name": "ct_dot_dieu_tri_iceberg",
        "schema": schema_ct_dot_dieu_tri,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.ct_dv_kham": {
        "table_name": "ct_dv_kham_iceberg",
        "schema": schema_ct_dv_kham,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.ct_dv_kham_ket_luan": {
        "table_name": "ct_dv_kham_ket_luan_iceberg",
        "schema": schema_ct_dv_kham_ket_luan,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.ct_dv_ky_thuat": {
        "table_name": "ct_dv_ky_thuat_iceberg",
        "schema": schema_ct_dv_ky_thuat,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.ct_kham_suc_khoe": {
        "table_name": "ct_kham_suc_khoe_iceberg",
        "schema": schema_ct_kham_suc_khoe,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.ct_nguon_nb": {
        "table_name": "ct_nguon_nb_iceberg",
        "schema": schema_ct_nguon_nb,
        "primary_keys": ["nb_dot_dieu_tri_id"]
    },
    "his.core_his_prod.ct_phieu_thu": {
        "table_name": "ct_phieu_thu_iceberg",
        "schema": schema_ct_phieu_thu,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_benh_nhan": {
        "table_name": "dm_benh_nhan_iceberg",
        "schema": schema_dm_benh_nhan,
        "primary_keys": ["nb_thong_tin_id"]
    },
    "his.core_his_prod.dm_bo_chi_dinh": {
        "table_name": "dm_bo_chi_dinh_iceberg",
        "schema": schema_dm_bo_chi_dinh,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_chuyen_khoa": {
        "table_name": "dm_chuyen_khoa_iceberg",
        "schema": schema_dm_chuyen_khoa,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_dich_vu": {
        "table_name": "dm_dich_vu_iceberg",
        "schema": schema_dm_dich_vu,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_doi_tuong_kcb": {
        "table_name": "dm_doi_tuong_kcb_iceberg",
        "schema": schema_dm_doi_tuong_kcb,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_dv_discount": {
        "table_name": "dm_dv_discount_iceberg",
        "schema": schema_dm_dv_discount,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_hoc_ham_hoc_vi": {
        "table_name": "dm_hoc_ham_hoc_vi_iceberg",
        "schema": schema_dm_hoc_ham_hoc_vi,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_hop_dong_ksk": {
        "table_name": "dm_hop_dong_ksk_iceberg",
        "schema": schema_dm_hop_dong_ksk,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_nguoi_gioi_thieu": {
        "table_name": "dm_nguoi_gioi_thieu_iceberg",
        "schema": schema_dm_nguoi_gioi_thieu,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_nguon_nb": {
        "table_name": "dm_nguon_nb_iceberg",
        "schema": schema_dm_nguon_nb,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_nhan_vien": {
        "table_name": "dm_nhan_vien_iceberg",
        "schema": schema_dm_nhan_vien,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_nhom_dich_vu_cap1": {
        "table_name": "dm_nhom_dich_vu_cap1_iceberg",
        "schema": schema_dm_nhom_dich_vu_cap1,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_nhom_dich_vu_cap2": {
        "table_name": "dm_nhom_dich_vu_cap2_iceberg",
        "schema": schema_dm_nhom_dich_vu_cap2,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_nhom_dich_vu_cap3": {
        "table_name": "dm_nhom_dich_vu_cap3_iceberg",
        "schema": schema_dm_nhom_dich_vu_cap3,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_phong": {
        "table_name": "dm_phong_iceberg",
        "schema": schema_dm_phong,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_quan_huyen": {
        "table_name": "dm_quan_huyen_iceberg",
        "schema": schema_dm_quan_huyen,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_tinh_thanh_pho": {
        "table_name": "dm_tinh_thanh_pho_iceberg",
        "schema": schema_dm_tinh_thanh_pho,
        "primary_keys": ["id"]
    },
    "his.core_his_prod.dm_xa_phuong": {
        "table_name": "dm_xa_phuong_iceberg",
        "schema": schema_dm_xa_phuong,
        "primary_keys": ["xa_phuong_id"]
    },
    "his.core_his_prod.hospital_configs": {
        "table_name": "hospital_configs_iceberg",
        "schema": schema_hospital_configs,
        "primary_keys": ["id"]
    }
}