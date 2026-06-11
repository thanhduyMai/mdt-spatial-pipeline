import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

def main():
    parser = argparse.ArgumentParser()
    # ĐỒNG BỘ 1: Chỉ nhận vào --date, bỏ hoàn toàn --hour
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    # ==================================================
    # H3 -> WKT POLYGON (UDF duy nhất phục vụ PostGIS)
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

    # Tối ưu: Đã xóa h3_lat_udf và h3_lon_udf vì File 4 đã tính sẵn và lưu ở tầng Gold!

    # ==================================================
    # SPARK SESSION 
    # ==================================================
    spark = SparkSession.builder \
        .appName(f"MDT_Serving_Postgres_Daily_{args.date}") \
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
        # ĐỒNG BỘ 2: Đọc dữ liệu cả ngày từ tầng Gold
        gold_df = spark.read.format("delta").load(input_path) \
                       .filter(col("date") == args.date)
                       
        if gold_df.count() > 0:
            # Sinh chuỗi hình học WKT cho PostGIS
            gold_df = gold_df.withColumn("h3_wkt", h3_polygon_udf(col("h3_index")))

            # ĐỒNG BỘ 3: Dọn sạch toàn bộ dữ liệu của NGÀY đó trong Postgres trước khi ghi đè
            delete_spatial_sql = f"DELETE FROM {pg_spatial_table} WHERE date = '{args.date}'"
            print(f" Đang dọn dẹp dữ liệu cũ của NGÀY {args.date} trong Postgres...")
            execute_postgres_query(delete_spatial_sql)

            print(f" Đang bơm dữ liệu không gian nguyên ngày vào bảng: {pg_spatial_table}...")
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
            print(f" Không có dữ liệu Gold ngày {args.date} để đẩy.")
    except Exception as e:
        print(f" Lỗi đẩy dữ liệu không gian: {str(e)}")

    # ==================================================
    # 2. READ & PUSH QUALITY AUDIT REPORT (SILVER)
    # ==================================================
    audit_path = "s3a://silver/quality_report/"
    pg_audit_table = "mdt_audit_report"

    try:
        # ĐỒNG BỘ 4: Đọc báo cáo kiểm định chất lượng cho cả ngày
        audit_df = spark.read.format("delta").load(audit_path) \
                        .filter(col("date") == args.date)

        if audit_df.count() > 0:
            # ĐỒNG BỘ 5: Xóa báo cáo Audit cũ theo NGÀY
            delete_audit_sql = f"DELETE FROM {pg_audit_table} WHERE date = '{args.date}'"
            execute_postgres_query(delete_audit_sql)

            print(f" Đang bơm Báo cáo chất lượng nguyên ngày vào bảng: {pg_audit_table}...")
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
            print(f" Không có báo cáo Quality Audit ngày {args.date} để đẩy.")
    except Exception as e:
        print(f" Bỏ qua luồng Audit: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    main()