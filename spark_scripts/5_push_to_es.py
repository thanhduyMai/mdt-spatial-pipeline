import argparse
import h3

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf,
    col,
    concat_ws
)
from pyspark.sql.types import StringType


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)

    args = parser.parse_args()

    # ==================================================
    # H3 -> CENTER POINT
    # ==================================================

    def get_h3_center(h3_idx):
        try:
            lat, lon = h3.cell_to_latlng(h3_idx)
            return f"{lat},{lon}"
        except:
            return None

    h3_center_udf = udf(
        get_h3_center,
        StringType()
    )

    # ==================================================
    # SPARK SESSION
    # ==================================================

    spark = SparkSession.builder \
        .appName(f"MDT_Push_To_ES_{args.date}_{args.hour}") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0") \
        .config("es.nodes", "elasticsearch") \
        .config("es.port", "9200") \
        .config("es.nodes.wan.only", "true") \
        .getOrCreate()

    # ==================================================
    # READ GOLD DATA
    # ==================================================

    input_path = (
        f"file:///opt/datalake/gold/mdt_aggregated/"
        f"date={args.date}/hour={args.hour}/"
    )

    gold_df = spark.read.parquet(input_path)

    # ==================================================
    # UNIQUE DOCUMENT ID
    # ==================================================

    if "record_date" in gold_df.columns and \
       "record_hour" in gold_df.columns:

        gold_df = gold_df.withColumn(
            "doc_id",
            concat_ws(
                "_",
                col("h3_index"),
                col("record_date"),
                col("record_hour")
            )
        )

    else:

        gold_df = gold_df.withColumn(
            "doc_id",
            concat_ws(
                "_",
                col("h3_index"),
                col("record_hour")
            )
        )

    # ==================================================
    # LOCATION FOR KIBANA MAP
    # ==================================================

    gold_df = gold_df.withColumn(
        "location",
        h3_center_udf(col("h3_index"))
    )

    # ==================================================
    # ES INDEX NAME
    # ==================================================

    month_prefix = args.date.replace(
        "-",
        ""
    )[:6]

    es_index = f"mdt_heatmap_{month_prefix}"

    # ==================================================
    # PUSH TO ELASTICSEARCH
    # ==================================================

    try:

        record_count = gold_df.count()

        print(
            f" Đang bơm {record_count} bản ghi "
            f"vào Elasticsearch Index: {es_index}..."
        )

        gold_df.write \
            .format("org.elasticsearch.spark.sql") \
            .mode("append") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.mapping.id", "doc_id") \
            .save(es_index)

        print(" PUSH ES THÀNH CÔNG!")

    except Exception as e:

        print(
            f" Lỗi đẩy Elasticsearch: {str(e)}"
        )

        import sys
        sys.exit(1)
     
    import argparse
import h3

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf,
    col,
    concat_ws
)
from pyspark.sql.types import StringType


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--date", required=True)
    parser.add_argument("--hour", required=True)

    args = parser.parse_args()

    # ==================================================
    # H3 -> CENTER POINT
    # ==================================================

    def get_h3_center(h3_idx):
        try:
            lat, lon = h3.cell_to_latlng(h3_idx)
            return f"{lat},{lon}"
        except:
            return None

    h3_center_udf = udf(
        get_h3_center,
        StringType()
    )

    # ==================================================
    # SPARK SESSION
    # ==================================================

    spark = SparkSession.builder \
        .appName(f"MDT_Push_To_ES_{args.date}_{args.hour}") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.0") \
        .config("es.nodes", "elasticsearch") \
        .config("es.port", "9200") \
        .config("es.nodes.wan.only", "true") \
        .getOrCreate()

    # ==================================================
    # READ GOLD DATA
    # ==================================================

    input_path = (
        f"file:///opt/datalake/gold/mdt_aggregated/"
        f"date={args.date}/hour={args.hour}/"
    )

    gold_df = spark.read.parquet(input_path)

    # ==================================================
    # UNIQUE DOCUMENT ID
    # ==================================================

    if "record_date" in gold_df.columns and \
       "record_hour" in gold_df.columns:

        gold_df = gold_df.withColumn(
            "doc_id",
            concat_ws(
                "_",
                col("h3_index"),
                col("record_date"),
                col("record_hour")
            )
        )

    else:

        gold_df = gold_df.withColumn(
            "doc_id",
            concat_ws(
                "_",
                col("h3_index"),
                col("record_hour")
            )
        )

    # ==================================================
    # LOCATION FOR KIBANA MAP
    # ==================================================

    gold_df = gold_df.withColumn(
        "location",
        h3_center_udf(col("h3_index"))
    )

    # ==================================================
    # ES INDEX NAME
    # ==================================================

    month_prefix = args.date.replace(
        "-",
        ""
    )[:6]

    es_index = f"mdt_heatmap_{month_prefix}"

    # ==================================================
    # PUSH TO ELASTICSEARCH
    # ==================================================

    try:

        record_count = gold_df.count()

        print(
            f" Đang bơm {record_count} bản ghi "
            f"vào Elasticsearch Index: {es_index}..."
        )

        gold_df.write \
            .format("org.elasticsearch.spark.sql") \
            .mode("append") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.mapping.id", "doc_id") \
            .save(es_index)

        print(" PUSH ES THÀNH CÔNG!")

    except Exception as e:

        print(
            f" Lỗi đẩy Elasticsearch: {str(e)}"
        )

        import sys
        sys.exit(1)
     
    # ==================================================
    # 2. PUSH QUALITY AUDIT REPORT (Sửa chuẩn Idempotency)
    # ==================================================
    audit_path = f"file:///opt/datalake/gold/quality_report/date={args.date}/hour={args.hour}/"
    audit_index = f"mdt_audit_{month_prefix}"
    
    try:
        audit_df = spark.read.parquet(audit_path)
        
        # FIX: Tạo audit_id độc nhất cho batch giờ (Ví dụ: 2026-06-01_07) 
        # Giúp việc chạy lại DAG bao nhiêu lần cũng không bị đúp dòng report
        audit_df = audit_df.withColumn(
            "audit_id",
            concat_ws("_", col("batch_date"), col("batch_hour"))
        )
        
        print(f" Đang bơm Audit Report vào ES Index: {audit_index}...")
        audit_df.write \
            .format("org.elasticsearch.spark.sql") \
            .mode("append") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.mapping.id", "audit_id") \
            .save(audit_index)
            
        print(" PUSH AUDIT REPORT THÀNH CÔNG!")
    except Exception as e:
        print(f" Bỏ qua đẩy Audit Report (Có thể do chưa có file): {str(e)}")
    spark.stop()


if __name__ == "__main__":
    main()