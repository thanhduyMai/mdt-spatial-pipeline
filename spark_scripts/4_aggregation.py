import argparse
import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    # ĐỒNG BỘ 1: Chỉ nhận vào --date để chuyển hẳn sang luồng DAILY
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    # Khởi tạo Spark với cấu hình kết nối MinIO và DELTA LAKE
    spark = (
        SparkSession.builder.appName(f"MDT_Aggregation_Daily_{args.date}")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Kích hoạt Delta Extension
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    enriched_input = "s3a://silver/mdt_enriched/"
    gold_output = "s3a://gold/mdt_aggregated/"

    # 1. ĐỌC DỮ LIỆU TỪ TẦNG SILVER (Lọc theo Ngày)
    try:
        # ĐỒNG BỘ 2: Lọc toàn bộ giờ trong ngày để xử lý 1 thể
        enriched_df = spark.read.format("delta").load(enriched_input) \
                               .filter(F.col("date") == args.date)
                               
        if enriched_df.count() == 0:
            print(f" Không có dữ liệu ngày {args.date} để tính toán KPI. Dừng luồng an toàn.")
            sys.exit(0)
    except Exception as e:
        print(f" Không tìm thấy bảng Delta tại {enriched_input} hoặc lỗi: {e}. Dừng luồng.")
        sys.exit(0)

    # 2. TÍNH TOÁN CÁC CHỈ SỐ KPI THEO Ô LƯỚI H3 VÀ CELL_ID (Đa tiến trình theo Giờ)
    rsrp_col = "p_cell_rsrp"
    
    # ĐỒNG BỘ 3: Đưa 'date' và 'hour' vào groupBy để Spark tự chia nhóm tính toán cho từng giờ
    gold_df = enriched_df.groupBy("date", "hour", "h3_index", "cell_id").agg(
        F.count("*").alias("user_density_count"),
        F.round(F.avg(rsrp_col), 2).alias("avg_rsrp"),
        F.expr(f"percentile_approx({rsrp_col}, 0.1)").alias("p10_rsrp"),
        F.round(F.avg("distance_to_cell_km"), 2).alias("avg_distance_km"),
        
        # KPI Tỷ lệ tín hiệu yếu (< -110 dBm)
        F.round(
            F.avg(F.when(F.col(rsrp_col) < -110, 1).otherwise(0)), 4
        ).alias("weak_signal_ratio"),
        
        # Lấy tọa độ Tâm H3 và tọa độ Trạm
        F.first("h3_center_lat").alias("h3_center_lat"),
        F.first("h3_center_lon").alias("h3_center_lon"),
        F.first("cell_lat").alias("cell_lat"),
        F.first("cell_lon").alias("cell_lon")
    )

    # 3. GHI DỮ LIỆU CHUẨN IDEMPOTENT XUỐNG TẦNG GOLD MỨC NGÀY
    # ĐỒNG BỘ 4: replaceWhere dọn dẹp và ghi đè trọn vẹn theo ngày
    replace_condition = f"date = '{args.date}'"
    
    print(f" 🚀 Đang tiến hành ghi đè dữ liệu ngày {args.date} xuống tầng GOLD...")
    gold_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", replace_condition) \
        .option("mergeSchema", "true") \
        .partitionBy("date", "hour") \
        .save(gold_output)
        
    print(f" ✅ Task 4: Aggregation DAILY cho ngày {args.date} hoàn tất xuất sắc!")

if __name__ == "__main__":
    main()