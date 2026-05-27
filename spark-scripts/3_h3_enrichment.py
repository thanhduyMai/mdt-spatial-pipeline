import argparse
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
import h3 # Thư viện h3-py của Uber

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("MDT_Spatial_Enrichment").getOrCreate()

    # 1. Định nghĩa UDF (User Defined Function) để Spark gọi được thư viện H3
    # Chọn resolution = 9 (diện tích khoảng 0.1 km2, cạnh 174m - rất chuẩn cho Telco)
    @F.udf(returnType=StringType())
    def get_h3_index(lat, lon):
        try:
            return h3.geo_to_h3(lat, lon, 9) 
        except:
            return None

    # 2. Đọc Data Sạch và Bảng Cấu hình Trạm
    clean_df = spark.read.parquet(f"hdfs://datalake/silver/mdt_clean/date={args.date}/hour={args.hour}/")
    cell_db_df = spark.read.parquet("hdfs://datalake/master_data/cell_db/") # Giả định anh Việt Anh cung cấp

    # 3. Gắn mã H3 vào từng dòng log
    enriched_df = clean_df.withColumn("h3_index", get_h3_index(F.col("latitude"), F.col("longitude")))

    # 4. Join với Cell DB để lấy tọa độ trạm phát sóng (dựa trên cell_id)
    joined_df = enriched_df.join(cell_db_df, on="cell_id", how="left")

    # 5. Tính khoảng cách Haversine (tính bằng km) ngay trên Spark SQL (Tối ưu hiệu năng cực mạnh)
    # Công thức toán học nội tại của Spark chạy nhanh hơn tự viết hàm Python
    R = 6371.0 # Bán kính Trái Đất (km)
    final_df = joined_df.withColumn(
        "distance_to_cell_km",
        F.acos(
            F.sin(F.radians(F.col("latitude"))) * F.sin(F.radians(F.col("cell_lat"))) + 
            F.cos(F.radians(F.col("latitude"))) * F.cos(F.radians(F.col("cell_lat"))) * F.cos(F.radians(F.col("cell_lon") - F.col("longitude")))
        ) * R
    )

    # 6. Ghi xuống Silver (Đã làm giàu)
    output_path = f"hdfs://datalake/silver/mdt_enriched/date={args.date}/hour={args.hour}/"
    final_df.write.mode("overwrite").parquet(output_path)
    print("✅ Spatial Enrichment hoàn tất!")

if __name__ == "__main__":
    main()