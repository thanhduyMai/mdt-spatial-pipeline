import argparse
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, ArrayType, FloatType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)
    args = parser.parse_args()

    # 1. Khởi tạo Spark Session tích hợp Delta Lake & MinIO
    spark = (
        SparkSession.builder.appName(f"MDT_Spatial_Enrichment_{args.date}_{args.hour}")
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

    # THÊM MỚI: UDF lấy tọa độ Tâm của H3
    @F.udf(returnType=ArrayType(FloatType()))
    def get_h3_center(h3_idx):
        try:
            if not h3_idx: return None
            import h3
            lat, lon = h3.cell_to_latlng(h3_idx)
            return [float(lat), float(lon)]
        except Exception:
            return None

    # Đổi đường dẫn thành GỐC của bảng (Không chứa partition cứng)
    clean_input = "s3a://silver/mdt_clean/"
    enriched_output = "s3a://silver/mdt_enriched/"

    print(f"🚀 Bắt đầu chạy Spatial Enrichment (JDBC) cho batch: {args.date} {args.hour}:00")

    try:
        # Bước A: Đọc dữ liệu từ bảng DELTA `mdt_clean` và lọc đúng partition
        clean_df = spark.read.format("delta").load(clean_input) \
                        .filter((F.col("date") == args.date) & (F.col("hour") == args.hour))

        if clean_df.count() == 0:
            print(" Không có dữ liệu sạch (Clean Data) để làm giàu. Bỏ qua batch này.")
            sys.exit(0)

        # Bước B: Đọc Master Data từ PostgreSQL qua JDBC (Giữ nguyên)
        try:
            cell_db_raw = (
                spark.read.format("jdbc")
                .option("url", "jdbc:postgresql://postgis-dw:5432/mdt_db")
                .option("dbtable", "cell_info")
                .option("user", "postgres")
                .option("password", "postgres")
                # PostgreSQL driver jar đã được cấu hình trong Airflow/Spark image
                .load()
            )

            cell_db_df = cell_db_raw.select(
                F.col("cell_code").alias("cell_id"),
                F.col("x_").alias("cell_lat"),
                F.col("y_").alias("cell_lon"),
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

        # Bước D: Join với danh mục trạm
        joined_df = enriched_df.join(cell_db_df, on="cell_id", how="left")

        # ====================================================================
        # BƯỚC HACK (THÊM MỚI): LẤP ĐẦY TỌA ĐỘ TRẠM BỊ THIẾU (NULL)
        # ====================================================================
        window_spec = Window.partitionBy("cell_id")

        df_filled = joined_df.withColumn(
            "est_cell_lat", F.avg("lat").over(window_spec)
        ).withColumn(
            "est_cell_lon", F.avg("lng").over(window_spec)
        ).withColumn(
            "cell_lat", F.coalesce(F.col("cell_lat"), F.col("est_cell_lat"))
        ).withColumn(
            "cell_lon", F.coalesce(F.col("cell_lon"), F.col("est_cell_lon"))
        ).drop("est_cell_lat", "est_cell_lon") # Xóa 2 cột tạm đi cho sạch data

        # Bước E: Tính khoảng cách Haversine (Lúc này 100% data đã có cell_lat/lon)
        R = 6371.0
        df_math = df_filled.withColumn("lat", F.col("lat").cast("double")) \
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

        # Bước F: GHI DỮ LIỆU CHUẨN DELTA LAKE IDEMPOTENT
        # Chỉ ghi đè phân vùng đang xử lý, bảo toàn dữ liệu các giờ khác
        final_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"date = '{args.date}' AND hour = '{args.hour}'") \
            .partitionBy("date", "hour") \
            .save(enriched_output)
            
        print(f" Task 3: Spatial Enrichment thành công rực rỡ! Dữ liệu Delta lưu tại S3.")

    except Exception as e:
        print(f" Luồng xử lý Stage 3 thất bại: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()