import argparse
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("MDT_Aggregation").getOrCreate()

    # 1. Đọc data đã gắn H3
    enriched_df = spark.read.parquet(f"hdfs://datalake/silver/mdt_enriched/date={args.date}/hour={args.hour}/")

    # 2. Thực hiện Aggregation (Gom nhóm)
    gold_df = enriched_df.groupBy("h3_index").agg(
        F.count("*").alias("user_density_count"),           # Mật độ khách hàng (Count)
        F.round(F.avg("rsrp"), 2).alias("avg_rsrp"),        # Sóng trung bình
        F.expr("percentile_approx(rsrp, 0.1)").alias("p10_rsrp"), # 10% khách hàng sóng yếu nhất (P10)
        F.round(F.avg("distance_to_cell_km"), 2).alias("avg_distance_km")
    )

    # 3. Thêm cột thời gian chuẩn để vẽ lên Dashboard
    final_gold_df = gold_df.withColumn("record_hour", F.lit(f"{args.date} {args.hour}:00:00"))

    # 4. GHI XUỐNG BẰNG BIẾN MỚI (Đã sửa đổi)
    final_gold_df.write.mode("overwrite").parquet(f"hdfs://datalake/gold/mdt_aggregated/date={args.date}/hour={args.hour}/")
    
    print("✅ Aggregation hoàn tất! Dữ liệu đã sẵn sàng ở vùng Gold.")

if __name__ == "__main__":
    main()