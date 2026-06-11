import argparse
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)
    args = parser.parse_args()

    # Khởi tạo Spark với cấu hình thư viện Elasticsearch
    # Docker service name for Elasticsearch
    spark = SparkSession.builder \
        .appName(f"MDT_Push_To_ES_{args.date}_{args.hour}") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0") \
        .config("es.nodes", "elasticsearch") \
        .config("es.port", "9200") \
        .config("es.index.auto.create", "true") \
        .getOrCreate()

    # 1. Đọc data Aggregated (vừa sinh ra từ File 4)
    input_path = f"file:///opt/datalake/gold/mdt_aggregated/date={args.date}/hour={args.hour}/"
    print(f"Đang đọc dữ liệu Gold từ: {input_path}")
    gold_df = spark.read.parquet(input_path)

    # 2. Định nghĩa tên Index trong Elasticsearch (Gộp theo tháng để dễ quản lý)
    # Ví dụ: mdt_heatmap_202605
    month_prefix = args.date.replace("-", "")[:6] 
    es_index = f"mdt_heatmap_{month_prefix}"
    
    # THÊM 2 DÒNG NÀY ĐỂ CHECK SỐ LƯỢNG DATA TRƯỚC KHI ĐẨY:
    print(f"====== SỐ LƯỢNG DÒNG DATA TÌM THẤY TẠI GOLD: {gold_df.count()} ======")
    gold_df.show(5)

    # 3. Ghi dữ liệu vào Elasticsearch
    # Lưu ý: Cột record_hour trong data sẽ giúp ES hiểu đây là chuỗi thời gian (Time-series)
    try:
        gold_df.write \
            .format("org.elasticsearch.spark.sql") \
            .mode("append") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.mapping.id", "h3_index") \
            .save(es_index)
            
        print(f"✅ Đã push data thành công lên Elasticsearch Index: {es_index}")
    except Exception as e:
        print(f"❌ Lỗi đẩy dữ liệu lên ES: {str(e)}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()