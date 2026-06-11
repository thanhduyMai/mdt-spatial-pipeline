import os
import sys
import time
import argparse
import paramiko
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

try:
    from airflow.hooks.base import BaseHook
except ImportError as e:
    print(f' Lỗi: Chưa cài đặt đủ thư viện ({e})')

def fetch_file_from_sftp(remote_file_path, local_file_name):
    """
    Hàm kết nối SFTP động: truyền vào đường dẫn file trên server và tên file muốn lưu ở local
    """
    conn_id = 'mdt_sftp_server'
    print(f' Đang đọc cấu hình {conn_id} từ Airflow để tải {local_file_name}...')
    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception as e:
        print(f' Không tìm thấy Connection: {e}')
        sys.exit(1)

    host = conn.host
    port = conn.port if conn.port else 2222
    user = conn.login
    password = conn.password

    local_dir = '/tmp/vdt_data/mdt'
    local_file = f'{local_dir}/{local_file_name}'
    os.makedirs(local_dir, exist_ok=True)

    print(f' Bắt đầu kéo file {remote_file_path} từ {user}@{host}:{port}...')
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        ssh.connect(hostname=host, port=port, username=user, password=password, timeout=15)
        sftp = ssh.open_sftp()
        
        start_time = time.time()
        sftp.get(remote_file_path, local_file)
        end_time = time.time()
        
        sftp.close()
        ssh.close()
        
        print(f' Tải thành công {local_file_name}! Thời gian: {end_time - start_time:.3f}s')
        return local_file
        
    except FileNotFoundError:
        print(f'\n LỖI: Không tìm thấy file {remote_file_path} trên server SFTP.')
        sys.exit(1)
    except Exception as e:
        print(f'\n LỖI TRONG QUÁ TRÌNH TẢI SFTP: {e}')
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Ngày chạy pipeline (YYYY-MM-DD)")
    args = parser.parse_args()

    # Xử lý format ngày từ YYYY-MM-DD sang DDMMYYYY cho tên file mdt_records
    date_obj = time.strptime(args.date, "%Y-%m-%d")
    ddmmyyyy = time.strftime("%d%m%Y", date_obj)
    
    # Định nghĩa đường dẫn file
    mdt_remote_path = f"/u01/vdt-data-de/mdt/mdt_records_{ddmmyyyy}.csv"
    cell_info_remote_path = "/u01/vdt-data-de/mdt/cell-info.csv"
    
    # 1. Tải cả 2 file từ SFTP về Local Worker
    mdt_local_path = fetch_file_from_sftp(mdt_remote_path, f"mdt_records_{ddmmyyyy}.csv")
    cell_info_local_path = fetch_file_from_sftp(cell_info_remote_path, "cell_info.csv")

    # 2. Cấu hình Spark Session kết hợp Delta Lake
    spark = (
        SparkSession.builder.appName(f"Ingest_Daily_{args.date}")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    # =========================================================================
    # TASK 1A: LƯU DATA DANH MỤC TRẠM (CELL_INFO) VÀO BRONZE
    # =========================================================================
    print(f" Đang đọc dữ liệu Master Data từ: {cell_info_local_path}")
    df_cell = spark.read.csv(f"file://{cell_info_local_path}", header=True, inferSchema=True)
    
    if "cell_code" in df_cell.columns:
        df_cell = df_cell.withColumnRenamed("cell_code", "cell_id")

    # Ghi đè file cell_info vào Bronze (Ghi đè vì là Master Data)
    print(" Đang tiến hành ghi đè Master Data Cell Info vào tầng Bronze...")
    df_cell.write \
        .format("delta") \
        .mode("overwrite") \
        .save("s3a://bronze/cell_info/")
    print(" Đã lưu Master Data (Cell Info) vào Bronze thành công!")

    # =========================================================================
    # TASK 1B: LƯU DATA LỊCH SỬ KẾT NỐI (MDT_RECORDS) VÀO BRONZE
    # =========================================================================
    print(f" Đang đọc dữ liệu MDT bằng Spark từ: {mdt_local_path}")
    df_mdt = spark.read.csv(f"file://{mdt_local_path}", header=True, inferSchema=True)

    if "cell_code" in df_mdt.columns:
        print(" Found 'cell_code', renaming to 'cell_id'...")
        df_mdt = df_mdt.withColumnRenamed("cell_code", "cell_id")

    # Tự động sinh cột date, hour từ cột time_ms
    df_dynamic = df_mdt.withColumn("date", F.from_unixtime(F.col("time_ms") / 1000, "yyyy-MM-dd")) \
                       .withColumn("hour", F.from_unixtime(F.col("time_ms") / 1000, "HH"))

    # Lọc bảo vệ dữ liệu đúng ngày đang chạy
    df_to_write = df_dynamic.filter(F.col("date") == F.lit(args.date))

    # GHI VÀO DELTA LAKE
    print(f" Đang tiến hành ghi đè phân vùng Delta Lake cho toàn bộ ngày: date='{args.date}'")
    df_to_write.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"date = '{args.date}'") \
        .partitionBy("date", "hour") \
        .save("s3a://bronze/mdt_raw/")
    
    print(f" Task 1: Ingest DAILY batch {args.date} (bao gồm cả Cell Info) Done successfully!")

if __name__ == "__main__":
    main()