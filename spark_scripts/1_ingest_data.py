import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit # Import thêm lit để tạo cột tĩnh

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)
    args = parser.parse_args()

    # 1. Cấu hình Spark Session kết hợp Delta Lake
    spark = (
        SparkSession.builder.appName(f"Ingest_{args.date}_{args.hour}")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Kích hoạt Delta Extension và Catalog
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    # 2. Định nghĩa đường dẫn gốc (Không chia folder cứng nữa)
    input_path = f"s3a://landingzone/mdt_records_{args.date}-{args.hour}.csv"
    output_path = "s3a://bronze/mdt_raw/"

    # Đọc dữ liệu CSV
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # 3. Đẩy date và hour vào thành CỘT DỮ LIỆU thực tế
    # Delta Lake sẽ tự động đọc các cột này để tạo Partition ngầm bên dưới
    df_with_partitions = df.withColumn("date", lit(args.date)) \
                           .withColumn("hour", lit(args.hour))

    # 4. GHI DỮ LIỆU CHUẨN IDEMPOTENT VỚI DELTA LAKE
    # Dùng replaceWhere: Chỉ ghi đè ĐÚNG phân vùng (date, hour) đang chạy.
    # Nếu chạy lại job này, nó chỉ xóa data cũ của giờ đó và đắp data mới vào.
    df_with_partitions.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"date = '{args.date}' AND hour = '{args.hour}'") \
        .partitionBy("date", "hour") \
        .save(output_path)
    
    print(f" Task 1: Ingest batch {args.date} - {args.hour} Done via Delta Lake!")

if __name__ == "__main__":
    main()