import argparse
from pyspark.sql import SparkSession

def main():
    # 1. Hứng tham số từ Airflow truyền vào
    parser = argparse.ArgumentParser(description="Ingest MDT Data to Bronze")
    parser.add_argument("--date", required=True, help="Ngày chạy (YYYY-MM-DD)")
    parser.add_argument("--hour", required=True, help="Giờ chạy (00-23)")
    args = parser.parse_args()

    target_date = args.date
    target_hour = args.hour

    print(f"Bắt đầu Ingest dữ liệu cho Batch: {target_date} {target_hour}:00")

    # 2. Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName(f"MDT_Ingestion_Bronze_{target_date}_{target_hour}") \
        .getOrCreate()

    # 3. Định nghĩa đường dẫn
    # Giả sử anh Việt Anh vứt file thô vào thư mục landing_zone theo cấu trúc này
    raw_file_path = f"file:///landing_zone/mdt_data_{target_date}_{target_hour}.csv"
    
    # Đích đến ở Data Lake (HDFS)
    bronze_output_path = f"hdfs://datalake/bronze/mdt_raw/date={target_date}/hour={target_hour}/"

    try:
        # 4. Đọc dữ liệu thô
        print(f"Đang đọc file từ: {raw_file_path}")
        raw_df = spark.read.csv(raw_file_path, header=True, inferSchema=True)

        # 5. Ghi vào Bronze Zone
        print(f"Đang ghi vào vùng Bronze: {bronze_output_path}")
        raw_df.write \
            .mode("overwrite") \
            .parquet(bronze_output_path)
            
        print("✅ Ingestion thành công!")
        
    except Exception as e:
        print(f"❌ Lỗi trong quá trình Ingestion: {str(e)}")
        import sys
        sys.exit(1) # Bắn lỗi để Airflow biết mà đánh dấu ĐỎ task này

if __name__ == "__main__":
    main()