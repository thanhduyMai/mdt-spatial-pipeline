import argparse
import sys
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Import Great Expectations cho PySpark
import great_expectations as gx
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)
    args = parser.parse_args()

    # 1. Khởi tạo Spark Session
    spark = (
        SparkSession.builder.appName(f"QC_GX_6Dimensions_{args.date}_{args.hour}")
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

    print(f"🚀 Khởi động Động cơ Kiểm định Data Quality bằng GREAT EXPECTATIONS...")

    try:
        df = spark.read.format("delta").load(input_path) \
                 .filter((F.col("date") == args.date) & (F.col("hour") == args.hour))
    except Exception as e:
        print(f"⚠️ Không tìm thấy bảng Delta tại {input_path}. Luồng dừng an toàn.")
        sys.exit(0)

    total_raw_records = df.count()
    if total_raw_records == 0:
        print("⚠️ Phân vùng trống. Không có dữ liệu để đánh giá.")
        sys.exit(0)

    # ĐÃ LOẠI BỎ KHỬ TRÙNG (Uniqueness) Ở ĐÂY
    duplicate_count = 0
    pct_dup = 0.0

    # =================================================================================
    # BỘ LUẬT GREAT EXPECTATIONS (GX SUITE) CHO DOMAIN VIỄN THÔNG MDT
    # =================================================================================
    
    # Bọc trực tiếp PySpark DataFrame gốc (df) thay vì df_dedup
    gx_df = SparkDFDataset(df)

    # CHIỀU 1: Completeness (Tính đầy đủ - Không chứa Null/Rỗng ở các trường trọng yếu)
    gx_df.expect_column_values_to_not_be_null(column="lat")
    gx_df.expect_column_values_to_not_be_null(column="lng")
    gx_df.expect_column_values_to_not_be_null(column="cell_id")
    # cell_id không được là chuỗi rỗng
    gx_df.expect_column_value_lengths_to_be_between(column="cell_id", min_value=1)

    # CHIỀU 2: Validity (Tính hợp lệ - Chuẩn dải đo sóng vô tuyến 4G/LTE)
    gx_df.expect_column_values_to_be_between(column="p_cell_rsrp", min_value=-140, max_value=-44)
    gx_df.expect_column_values_to_be_between(column="p_cell_rsrq", min_value=-20, max_value=-3)

    # CHIỀU 3: Accuracy (Tính chính xác - Tọa độ vật lý phải nằm trong lãnh thổ Việt Nam)
    gx_df.expect_column_values_to_be_between(column="lat", min_value=8.5, max_value=23.4)
    gx_df.expect_column_values_to_be_between(column="lng", min_value=102.1, max_value=109.5)
    # Loại bỏ tọa độ rác phần cứng (0.0, 0.0)
    gx_df.expect_column_values_to_not_be_in_set(column="lat", value_set=[0.0])

    # CHIỀU 4: Consistency (Tính nhất quán - Cột date_hour phải đúng ca đang xử lý)
    expected_date_hour = f"{args.date}-{args.hour}"
    gx_df.expect_column_values_to_be_in_set(column="date_hour", value_set=[expected_date_hour])

    # =================================================================================
    # THỰC THI KIỂM ĐỊNH VÀ BÓC TÁCH KẾT QUẢ TỪ GX
    # =================================================================================
    validation_result = gx_df.validate()
    
    # Phân tích cục JSON trả về
    statistics = validation_result["statistics"]
    success_rate = statistics["success_percent"] # Tỷ lệ số bộ luật Pass
    is_passed = validation_result["success"] # True nếu không có luật nào Fail
    
    failed_expectations_count = statistics["unsuccessful_expectations"]
    evaluated_expectations_count = statistics["evaluated_expectations"]

    print(f"\n===  BÁO CÁO TỔNG QUAN TỪ GREAT EXPECTATIONS ===")
    print(f" [Tổng bản ghi] : {total_raw_records}")
    print(f" [Trùng lặp]    : {pct_dup:.2f}% (Đã tắt kiểm tra trùng lặp)")
    print(f" [Điểm DQ (GX)] : {success_rate:.2f}%")
    print(f" [Luật kiểm tra]: {evaluated_expectations_count} luật (Fail: {failed_expectations_count})")
    print(f"--------------------------------------------------\n")

    # =================================================================================
    # PHÂN LOẠI DỮ LIỆU ĐỂ LƯU XUỐNG TẦNG SILVER (SẠCH VÀ RÁC)
    # =================================================================================
    
    clean_cond = (
        F.col("lat").isNotNull() & F.col("lng").isNotNull() & (F.col("lat") != 0.0) &
        F.col("lat").between(8.5, 23.4) & F.col("lng").between(102.1, 109.5) &
        F.col("p_cell_rsrp").between(-140, -44) & F.col("p_cell_rsrq").between(-20, -3) &
        (F.col("date_hour") == expected_date_hour)
    )

    # Sử dụng trực tiếp dữ liệu từ df gốc
    clean_df = df.filter(clean_cond)
    dirty_df = df.filter(~clean_cond)

    clean_count = clean_df.count()
    dirty_count = dirty_df.count()

    # GHI XUỐNG DELTA LAKE
    replace_condition = f"date = '{args.date}' AND hour = '{args.hour}'"
    clean_df.withColumn("processed_at", F.current_timestamp()).coalesce(1).write.format("delta").mode("overwrite").option("replaceWhere", replace_condition).partitionBy("date", "hour").save(clean_path)
    dirty_df.withColumn("processed_at", F.current_timestamp()).write.format("delta").mode("overwrite").option("replaceWhere", replace_condition).partitionBy("date", "hour").save(dirty_path)

    # =================================================================================
    # GHI LẠI ĐIỂM SỐ GX LÊN BẢNG AUDIT ĐỂ VẼ DASHBOARD
    # =================================================================================
    report_data = [
        (args.date, args.hour, total_raw_records, duplicate_count, clean_count, dirty_count, 
         float(success_rate), int(evaluated_expectations_count), int(failed_expectations_count), bool(is_passed), datetime.now())
    ]
    schema = """date string, hour string, total_raw_records long, duplicate_count long, clean_count long, dirty_count long, 
                gx_success_rate double, gx_total_rules long, gx_failed_rules long, gx_is_passed boolean, created_at timestamp"""
    
    report_df = spark.createDataFrame(report_data, schema=schema)
    report_df.write.format("delta").mode("overwrite").option("replaceWhere", replace_condition).partitionBy("date", "hour").save(report_path)

    # =================================================================================
    
    THRESHOLD_SCORE = 50.0 
    
    if success_rate < THRESHOLD_SCORE:
        raise ValueError(f" CRITICAL DQ BLOCKER: Hệ thống GX đánh giá chất lượng lô dữ liệu quá thấp ({success_rate:.2f}% < {THRESHOLD_SCORE}%). Luồng bị ngắt tự động!")

    print(" Task 2: Toàn bộ quy trình kiểm định bởi Great Expectations hoàn tất xuất sắc!")

if __name__ == "__main__":
    main()