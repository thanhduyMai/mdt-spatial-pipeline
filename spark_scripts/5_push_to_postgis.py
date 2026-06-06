import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)
    args = parser.parse_args()

    # ==================================================
    # H3 -> WKT POLYGON (UDF phục vụ PostGIS)
    # ==================================================
    def get_h3_polygon_wkt(h3_idx):
        try:
            if h3_idx is None:
                return None
            import h3
            boundary = h3.cell_to_boundary(h3_idx)
            coords = [f"{lon} {lat}" for lat, lon in boundary]
            coords.append(f"{boundary[0][1]} {boundary[0][0]}")
            return f"POLYGON(({', '.join(coords)}))"
        except Exception:
            return None

    h3_polygon_udf = udf(get_h3_polygon_wkt, StringType())

    # ==================================================
    # [ĐÃ SỬA]: UDF Lấy tọa độ Tâm H3 (Dùng h3.cell_to_latlng của bản v4)
    # ==================================================
    def get_h3_center_lat(h3_idx):
        try:
            if h3_idx is None:
                return None
            import h3
            # cell_to_latlng trả về tuple (lat, lon)
            return float(h3.cell_to_latlng(h3_idx)[0])
        except Exception:
            return None

    def get_h3_center_lon(h3_idx):
        try:
            if h3_idx is None:
                return None
            import h3
            return float(h3.cell_to_latlng(h3_idx)[1])
        except Exception:
            return None

    h3_lat_udf = udf(get_h3_center_lat, StringType())
    h3_lon_udf = udf(get_h3_center_lon, StringType())

    # ==================================================
    # SPARK SESSION (Chuẩn Delta & Đã xóa packages)
    # ==================================================
    spark = SparkSession.builder \
        .appName(f"MDT_Serving_Postgres_{args.date}_{args.hour}") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # ==================================================
    # CẤU HÌNH KẾT NỐI POSTGIS
    # ==================================================
    pg_url = "jdbc:postgresql://postgis-dw:5432/mdt_db" 
    pg_user = "postgres"
    pg_password = "postgres"

    def execute_postgres_query(query):
        try:
            driver_manager = spark._jvm.java.sql.DriverManager
            conn = driver_manager.getConnection(pg_url, pg_user, pg_password)
            stmt = conn.createStatement()
            stmt.executeUpdate(query)
            stmt.close()
            conn.close()
        except Exception as e:
            print(f" Không thể thực thi lệnh tiền xử lý SQL: {str(e)}")

    # ==================================================
    # 1. READ & PUSH SPATIAL AGGREGATED DATA (GOLD)
    # ==================================================
    input_path = "s3a://gold/mdt_aggregated/"
    pg_spatial_table = "mdt_spatial_gold"

    try:
        # Đọc chuẩn Delta và lọc đúng giờ
        gold_df = spark.read.format("delta").load(input_path) \
                       .filter((col("date") == args.date) & (col("hour") == args.hour))
                       
        if gold_df.count() > 0:
            gold_df = gold_df.withColumn("h3_wkt", h3_polygon_udf(col("h3_index")))

            # Sinh tọa độ tâm H3
            gold_df = gold_df.withColumn("h3_center_lat", h3_lat_udf(col("h3_index")).cast("double"))
            gold_df = gold_df.withColumn("h3_center_lon", h3_lon_udf(col("h3_index")).cast("double"))

            delete_spatial_sql = f"DELETE FROM {pg_spatial_table} WHERE date = '{args.date}' AND hour = '{args.hour}'"
            print(f"Đang dọn dẹp dữ liệu cũ của ca {args.date} {args.hour}:00 trong Postgres...")
            execute_postgres_query(delete_spatial_sql)

            print(f"Đang bơm dữ liệu không gian vào bảng: {pg_spatial_table}...")
            gold_df.write \
                .format("jdbc") \
                .option("url", pg_url) \
                .option("dbtable", pg_spatial_table) \
                .option("user", pg_user) \
                .option("password", pg_password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print(" Đẩy dữ liệu không gian vào Postgres THÀNH CÔNG!")
        else:
            print(" Không có dữ liệu Gold để đẩy.")
    except Exception as e:
        print(f" Lỗi đẩy dữ liệu không gian: {str(e)}")

    # ==================================================
    # 2. READ & PUSH QUALITY AUDIT REPORT (SILVER)
    # ==================================================
    audit_path = "s3a://silver/quality_report/"
    pg_audit_table = "mdt_audit_report"

    try:
        audit_df = spark.read.format("delta").load(audit_path) \
                         .filter((col("date") == args.date) & (col("hour") == args.hour))

        if audit_df.count() > 0:
            delete_audit_sql = f"DELETE FROM {pg_audit_table} WHERE date = '{args.date}' AND hour = '{args.hour}'"
            execute_postgres_query(delete_audit_sql)

            print(f" Đang bơm Báo cáo chất lượng vào bảng: {pg_audit_table}...")
            audit_df.write \
                .format("jdbc") \
                .option("url", pg_url) \
                .option("dbtable", pg_audit_table) \
                .option("user", pg_user) \
                .option("password", pg_password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print(" Đẩy báo cáo Audit vào Postgres THÀNH CÔNG!")
        else:
            print(" Không có báo cáo Quality Audit để đẩy.")
    except Exception as e:
        print(f" Bỏ qua luồng Audit: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    main()