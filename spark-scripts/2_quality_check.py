import argparse
import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def main():
    # 1. Hứng tham số (Tương tự File 1)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)
    args = parser.parse_args()

    target_date = args.date
    target_hour = args.hour

    spark = SparkSession.builder \
        .appName(f"MDT_QualityCheck_{target_date}_{target_hour}") \
        .getOrCreate()

    # 2. Định nghĩa các đường dẫn
    bronze_input = f"hdfs://datalake/bronze/mdt_raw/date={target_date}/hour={target_hour}/"
    silver_clean = f"hdfs://datalake/silver/mdt_clean/date={target_date}/hour={target_hour}/"
    silver_quarantine = f"hdfs://datalake/silver/mdt_quarantine/date={target_date}/hour={target_hour}/"

    print(f"Bắt đầu Quality Check cho luồng: {target_date} {target_hour}h")

    # 3. Đọc dữ liệu từ Bronze và nạp lên RAM (Cache)
    df = spark.read.parquet(bronze_input)
    df.cache() # Tối quan trọng: Tránh việc Spark phải đọc lại ổ cứng nhiều lần

    # 4. Thiết lập Luật kiểm định (Quality Rules)
    # Tọa độ Việt Nam (Lat: ~8.5 đến 23.4, Long: ~102.1 đến 109.5)
    # RSRP vật lý: -140 đến -44
    is_valid_rule = (
        (F.col("rsrp").between(-140, -44)) & 
        (F.col("latitude").between(8.5, 23.4)) & 
        (F.col("longitude").between(102.1, 109.5)) &
        (F.col("cell_id").isNotNull())
    )

    # 5. Phân tách Dữ liệu (Branching)
    # Nhánh Sạch
    clean_df = df.filter(is_valid_rule)
    clean_df.write.mode("overwrite").parquet(silver_clean)

    # Nhánh Rác (Phủ định của luật trên bằng dấu ~)
    bad_df = df.filter(~is_valid_rule)
    bad_df.write.mode("overwrite").parquet(silver_quarantine)

    # 6. Tính toán Metric & Tỷ lệ lỗi (Audit)
    total_records = df.count()
    bad_records = bad_df.count()
    
    df.unpersist() # Xử lý xong thì giải phóng RAM

    error_rate = (bad_records / total_records) * 100 if total_records > 0 else 0
    
    print(f"Tổng số bản ghi: {total_records}")
    print(f"Số bản ghi lỗi: {bad_records}")
    print(f"Tỷ lệ lỗi (Error Rate): {error_rate:.2f}%")

    # [Tùy chọn: Đoạn này bạn có thể insert 1 dòng vào bảng Delta Lake để lưu Log]

    # 7. GATEKEEPER (Người gác cổng)
    if error_rate > 15.0:
        print("🚨 CRITICAL ALERT: Tỷ lệ lỗi vượt quá 15%! Ngắt toàn bộ hệ thống.")
        sys.exit(1) # Lệnh này làm Spark "chết", Airflow sẽ bắt được và báo task màu ĐỎ
    else:
        print("✅ Dữ liệu đạt chuẩn. Cho phép Pipeline đi tiếp sang vùng Silver.")
        sys.exit(0) # Thành công

if __name__ == "__main__":
    main()