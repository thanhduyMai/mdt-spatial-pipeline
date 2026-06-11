import argparse
import sys
import time
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType, FloatType
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    # Chỉ nhận vào tham số --date để chạy mức Ngày (Daily)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    # 1. Khởi tạo Spark Session tích hợp Delta Lake & MinIO mức Ngày
    spark = (
        SparkSession.builder.appName(f"MDT_Spatial_Enrichment_Daily_{args.date}")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # Tự động gộp file nhỏ chống nghẽn ổ cứng (I/O)
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .getOrCreate()
    )

    # 2. Định nghĩa UDF tính H3 Index
    @F.udf(returnType=StringType())
    def get_h3_index(lat, lon):
        try:
            if lat is None or lon is None:
                return None
            import h3
            return h3.latlng_to_cell(float(lat), float(lon), 9)
        except Exception:
            return None

    # UDF lấy tọa độ Tâm của H3
    @F.udf(returnType=ArrayType(FloatType()))
    def get_h3_center(h3_idx):
        try:
            if not h3_idx: return None
            import h3
            lat, lon = h3.cell_to_latlng(h3_idx)
            return [float(lat), float(lon)]
        except Exception:
            return None

    clean_input = "s3a://silver/mdt_clean/"
    enriched_output = "s3a://silver/mdt_enriched/"

    print(f"🚀 Bắt đầu chạy Spatial Enrichment (DAILY MODE - NO HACK) cho ngày: {args.date}")

    try:
        # Bước A: Đọc dữ liệu từ bảng DELTA mdt_clean và lọc theo ngày
        clean_df = spark.read.format("delta").load(clean_input) \
                        .filter(F.col("date") == args.date)

        if clean_df.count() == 0:
            print(f" Không có dữ liệu sạch (Clean Data) để làm giàu cho ngày {args.date}. Bỏ qua batch này.")
            sys.exit(0)

        # Bước B: Đọc Master Data từ PostgreSQL qua JDBC
        try:
            cell_db_raw = (
                spark.read.format("jdbc")
                .option("url", "jdbc:postgresql://postgis-dw:5432/mdt_db")
                .option("dbtable", "cell_info")
                .option("user", "postgres")
                .option("password", "postgres")
                .load()
            )

            cell_db_df = cell_db_raw.select(
                F.col("cell_code").alias("cell_id"),
                F.col("x").alias("cell_lat"),
                F.col("y").alias("cell_lon"),
            )
            print(" Kết nối JDBC thành công! Đã lấy danh mục trạm từ PostgreSQL.")

        except Exception as e:
            error_msg = f" THẤT BẠI JDBC: Không thể đọc dữ liệu từ PostgreSQL. Chi tiết: {e}"
            print(error_msg)
            raise ConnectionError(error_msg)

        # Bước C: Gắn mã H3 Index và Tọa độ tâm H3
        enriched_df = clean_df.withColumn(
            "h3_index", get_h3_index(F.col("lat"), F.col("lng"))
        ).withColumn(
            "h3_center_coords", get_h3_center(F.col("h3_index"))
        ).withColumn(
            "h3_center_lat", F.col("h3_center_coords")[0]
        ).withColumn(
            "h3_center_lon", F.col("h3_center_coords")[1]
        ).drop("h3_center_coords")

        # Bước D: Join với danh mục trạm từ PostGIS
        joined_df = enriched_df.join(cell_db_df, on="cell_id", how="left")

        # Bước E: Ép kiểu dữ liệu phục vụ tính toán khoảng cách Haversine thẳng từ joined_df
        R = 6371.0
        df_math = joined_df.withColumn("lat", F.col("lat").cast("double")) \
                           .withColumn("lng", F.col("lng").cast("double")) \
                           .withColumn("cell_lat", F.col("cell_lat").cast("double")) \
                           .withColumn("cell_lon", F.col("cell_lon").cast("double"))

        final_df = df_math.withColumn(
            "distance_to_cell_km",
            F.acos(
                F.sin(F.radians(F.col("lat"))) * F.sin(F.radians(F.col("cell_lat"))) + 
                F.cos(F.radians(F.col("lat"))) * F.cos(F.radians(F.col("cell_lat"))) * F.cos(F.radians(F.col("cell_lon") - F.col("lng")))
            ) * R
        )

        # Bước F: GHI DỮ LIỆU CHUẨN DELTA LAKE IDEMPOTENT MỨC NGÀY
        replace_condition = f"date = '{args.date}'"
        
        print(f" Đang tiến hành ghi đè toàn bộ ngày vào tầng Enriched: {args.date}")
        final_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", replace_condition) \
            .partitionBy("date", "hour") \
            .save(enriched_output)
            
        # THỰC THI Z-ORDER THEO H3_INDEX CHO PHÂN VÙNG NGÀY VỪA GHI
        print(f"🔄 Đang tối ưu hóa không gian (Z-ORDER) theo h3_index cho ngày {args.date}...")
        spark.sql(f"OPTIMIZE delta.`{enriched_output}` WHERE date = '{args.date}' ZORDER BY (h3_index)")
            
        print(f"✅ Task 3: Spatial Enrichment và Z-Order tối ưu DAILY hoàn thành rực rỡ!")

    except Exception as e:
        print(f" Luồng xử lý Stage 3 thất bại: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()