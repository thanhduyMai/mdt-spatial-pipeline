import argparse
import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)
    args = parser.parse_args()

    # Khởi tạo Spark với cấu hình kết nối MinIO và DELTA LAKE
    spark = (
        SparkSession.builder.appName(f"MDT_Aggregation_{args.date}_{args.hour}")
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

    # ĐỊNH NGHĨA CÁC ĐƯỜNG DẪN GỐC
    enriched_input = "s3a://silver/mdt_enriched/"
    gold_output = "s3a://gold/mdt_aggregated/"

    # 1. ĐỌC DỮ LIỆU TỪ TẦNG SILVER (Bằng Delta)
    try:
        enriched_df = spark.read.format("delta").load(enriched_input) \
                           .filter((F.col("date") == args.date) & (F.col("hour") == args.hour))
                           
        if enriched_df.count() == 0:
            print(" Không có dữ liệu để tính toán KPI. Dừng luồng an toàn.")
            sys.exit(0)
    except Exception as e:
        print(f" Không tìm thấy bảng Delta tại {enriched_input}. Dừng luồng an toàn.")
        sys.exit(0)

    # 2. TÍNH TOÁN CÁC CHỈ SỐ KPI THEO Ô LƯỚI H3 VÀ CELL_ID
    rsrp_col = "p_cell_rsrp"
    
    # [CHUẨN]: Đưa cell_id vào groupBy, dùng F.first để giữ tọa độ
    gold_df = enriched_df.groupBy("h3_index", "cell_id").agg(
        F.count("*").alias("user_density_count"),
        F.round(F.avg(rsrp_col), 2).alias("avg_rsrp"),
        F.expr(f"percentile_approx({rsrp_col}, 0.1)").alias("p10_rsrp"),
        F.round(F.avg("distance_to_cell_km"), 2).alias("avg_distance_km"),
        # KPI Tỷ lệ tín hiệu yếu (< -110 dBm)
        F.round(
            F.avg(F.when(F.col(rsrp_col) < -110, 1).otherwise(0)), 4
        ).alias("weak_signal_ratio"),
        
        # =========================================================
        # BỔ SUNG QUAN TRỌNG: Lấy tọa độ Tâm H3 từ file 3 truyền sang
        # =========================================================
        F.first("h3_center_lat").alias("h3_center_lat"),
        F.first("h3_center_lon").alias("h3_center_lon"),

        # Giữ lại tọa độ của trạm (Lúc này đã có 100% data nhờ mẹo ở file 3)
        F.first("cell_lat").alias("cell_lat"),
        F.first("cell_lon").alias("cell_lon")
    )

    # ĐỔI TÊN THÀNH `date` VÀ `hour` ĐỂ CHUẨN BỊ LÀM PARTITION DELTA
    gold_df = gold_df.withColumn("date", F.lit(args.date)).withColumn("hour", F.lit(args.hour))

    # --- [ĐÃ XÓA HOÀN TOÀN LOGIC BASELINE 7 NGÀY] ---

    # 3. GHI DỮ LIỆU CHUẨN IDEMPOTENT XUỐNG TẦNG GOLD
    gold_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"date = '{args.date}' AND hour = '{args.hour}'") \
        .option("mergeSchema", "true") \
        .partitionBy("date", "hour") \
        .save(gold_output)
        
    print(f" Task 4: Aggregation cho {args.date} {args.hour}:00 hoàn tất xuất sắc (Clean - No Baseline)!")

if __name__ == "__main__":
    main()