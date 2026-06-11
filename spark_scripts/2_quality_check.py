import argparse
import sys
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.window import Window  # Phục vụ tính toán Dedup
from pyspark.sql import SparkSession

# Import Great Expectations cho PySpark
import great_expectations as gx
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

def main():
    parser = argparse.ArgumentParser()
    # THAY ĐỔI 1: Chỉ nhận vào tham số --date, bỏ hẳn --hour để chuyển sang chạy Daily
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    # 1. Khởi tạo Spark Session tích hợp Delta Lake mức Ngày
    spark = (
        SparkSession.builder.appName(f"QC_GX_6Dimensions_Daily_{args.date}")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    input_path = "s3a://bronze/mdt_raw/"
    clean_path = "s3a://silver/mdt_clean/"
    dirty_path = "s3a://silver/dirty_data/" 
    report_path = "s3a://silver/quality_report/"

    print(f"🚀 Khởi động Động cơ Kiểm định Data Quality thực tế bằng GREAT EXPECTATIONS (DAILY MODE)...")

    # THAY ĐỔI 2: Chỉ lọc dữ liệu theo Ngày từ tầng Bronze
    try:
        df = spark.read.format("delta").load(input_path) \
                 .filter(F.col("date") == args.date)
    except Exception as e:
        print(f"⚠️ Không tìm thấy bảng Delta tại {input_path}. Luồng dừng an toàn.")
        sys.exit(0)

    total_raw_records = df.count()
    if total_raw_records == 0:
        print("⚠️ Phân vùng trống. Không có dữ liệu để đánh giá.")
        sys.exit(0)

    # =================================================================================
    # THAY ĐỔI 3: ĐỔI ĐÚNG TÊN CỘT THEO BRONZE SCHEMA VÀ TẠO COMPOSITE KEY DEDUP
    # =================================================================================
    # Đã đổi: gcell_code -> cell_id, nr_rsrp -> p_cell_rsrp, nr_rsrq -> p_cell_rsrq, bỏ imsi
    unique_columns = ["time_ms", "lat", "lng", "cell_id", "p_cell_rsrp", "p_cell_rsrq"]
    
    window_spec = Window.partitionBy(unique_columns).orderBy(F.lit(1))
    df_with_rn = df.withColumn("rn", F.row_number().over(window_spec))

    # Đếm số lượng hàng trùng lặp
    duplicate_count = df_with_rn.filter(F.col("rn") > 1).count()
    pct_dup = (duplicate_count / total_raw_records) * 100 if total_raw_records > 0 else 0.0

    # =================================================================================
    # BỘ LUẬT GREAT EXPECTATIONS (GX SUITE) - ĐÃ CẬP NHẬT TÊN CỘT CHUẨN
    # =================================================================================
    gx_df = SparkDFDataset(df)

    # CHIỀU 1: Completeness
    gx_df.expect_column_values_to_not_be_null(column="lat", mostly=0.95)
    gx_df.expect_column_values_to_not_be_null(column="lng", mostly=0.95)
    gx_df.expect_column_values_to_not_be_null(column="cell_id", mostly=1.0)
    gx_df.expect_column_value_lengths_to_be_between(column="cell_id", min_value=1, mostly=1.0)

    # CHIỀU 2: Validity
    gx_df.expect_column_values_to_be_between(column="p_cell_rsrp", min_value=-140, max_value=-44, mostly=0.98)
    gx_df.expect_column_values_to_be_between(column="p_cell_rsrq", min_value=-20, max_value=-3, mostly=0.98)

    # CHIỀU 3: Accuracy
    gx_df.expect_column_values_to_be_between(column="lat", min_value=8.5, max_value=23.4, mostly=0.95)
    gx_df.expect_column_values_to_be_between(column="lng", min_value=102.1, max_value=109.5, mostly=0.95)
    gx_df.expect_column_values_to_not_be_in_set(column="lat", value_set=[0.0], mostly=0.95)

    # CHIỀU 4: Consistency (Kiểm tra định dạng chuỗi date_hour phải bắt đầu bằng ngày chạy)
    # Ví dụ: 2026-05-05-00, 2026-05-05-01,...
    gx_df.expect_column_values_to_match_regex(column="date_hour", regex=f"^{args.date}-\\d{{2}}$", mostly=1.0)

    # CHIỀU 5: Uniqueness
    gx_df.expect_compound_columns_to_be_unique(column_list=unique_columns, mostly=0.98)

    # =================================================================================
    # THỰC THI KIỂM ĐỊNH VÀ TRÍCH XUẤT ĐIỂM DQ TỪ GX
    # =================================================================================
    validation_result = gx_df.validate()
    
    statistics = validation_result["statistics"]
    success_rate = statistics["success_percent"]
    is_passed = validation_result["success"]
    
    failed_expectations_count = statistics["unsuccessful_expectations"]
    evaluated_expectations_count = statistics["evaluated_expectations"]

    print(f"\n=== BÁO CÁO CHẤT LƯỢNG DỮ LIỆU DAILY (GX PRODUCTION) ===")
    print(f" [Tổng bản ghi] : {total_raw_records}")
    print(f" [Trùng lặp thực tế] : {duplicate_count} hàng ({pct_dup:.2f}%)")
    print(f" [Điểm DQ (GX Rule Score)] : {success_rate:.2f}%")
    print(f" [Tổng số luật] : {evaluated_expectations_count} luật (Vi phạm ngưỡng: {failed_expectations_count})")
    print(f"--------------------------------------------------\n")

    # =================================================================================
    # THAY ĐỔI 4: ĐIỀU KIỆN CHẮN RÁC QUA TẦNG SILVER (SỬ DỤNG TÊN CỘT MỚI)
    # =================================================================================
    clean_cond = (
        F.col("lat").isNotNull() & F.col("lng").isNotNull() & (F.col("lat") != 0.0) &
        F.col("lat").between(8.5, 23.4) & F.col("lng").between(102.1, 109.5) &
        F.col("p_cell_rsrp").between(-140, -44) & F.col("p_cell_rsrq").between(-20, -3) &
        F.col("date_hour").rlike(f"^{args.date}-\\d{{2}}$") &
        (F.col("rn") == 1)
    )

    clean_df = df_with_rn.filter(clean_cond).drop("rn")
    dirty_df = df_with_rn.filter(~clean_cond).drop("rn")

    clean_count = clean_df.count()
    dirty_count = dirty_df.count()

    # THAY ĐỔI 5: GHI ĐÈ IDEMPOTENT THEO NGÀY (replaceWhere chỉ check date)
    # Vẫn dùng partitionBy("date", "hour") để Silver tự động chia folder nhỏ theo giờ bên trong
    replace_condition = f"date = '{args.date}'"
    
    print(f" Đang tiến hành ghi đè phân vùng Silver Delta Lake cho ngày: {args.date}")
    clean_df.withColumn("processed_at", F.current_timestamp()).write.format("delta").mode("overwrite").option("replaceWhere", replace_condition).partitionBy("date", "hour").save(clean_path)
    dirty_df.withColumn("processed_at", F.current_timestamp()).write.format("delta").mode("overwrite").option("replaceWhere", replace_condition).partitionBy("date", "hour").save(dirty_path)

    # =================================================================================
    # THAY ĐỔI 6: LƯU BÁO CÁO AUDIT THEO NGÀY (Gán cột hour cố định bằng 'ALL')
    # =================================================================================
    report_data = [
        (args.date, "ALL", total_raw_records, duplicate_count, clean_count, dirty_count, 
         float(success_rate), int(evaluated_expectations_count), int(failed_expectations_count), bool(is_passed), datetime.now())
    ]
    schema = """date string, hour string, total_raw_records long, duplicate_count long, clean_count long, dirty_count long, 
                gx_success_rate double, gx_total_rules long, gx_failed_rules long, gx_is_passed boolean, created_at timestamp"""
    
    report_df = spark.createDataFrame(report_data, schema=schema)
    
    # Đồng bộ replaceWhere và partitionBy theo cấu trúc bảng chung để bảo toàn tính idempotent
    report_df.write.format("delta").mode("overwrite").option("replaceWhere", f"date = '{args.date}' AND hour = 'ALL'").partitionBy("date", "hour").save(report_path)

    # =================================================================================
    # NGẮT TỰ ĐỘNG KHẨN CẤP (THRESHOLD BLOCKER)
    # =================================================================================
    THRESHOLD_SCORE = 80.0 
    if success_rate < THRESHOLD_SCORE:
        raise ValueError(f"  CRITICAL DQ BLOCKER: Chất lượng lô dữ liệu Ngày giảm sút nghiêm trọng vượt ngưỡng an toàn ({success_rate:.2f}% < {THRESHOLD_SCORE}%). Luồng xử lý bị ngắt!")

    print(f" Task 2: Quy trình kiểm định chất lượng & Khử trùng lặp DAILY cho ngày {args.date} hoàn tất xuất sắc!")

if __name__ == "__main__":
    main()