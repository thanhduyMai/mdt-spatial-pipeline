from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# 1. Cấu hình mặc định
default_args = {
    'owner': 'data_engineer_team',
    'depends_on_past': True, # Bắt buộc batch giờ trước phải xong mới chạy batch giờ sau
    'start_date': datetime(2026, 5, 20),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# 2. Khởi tạo DAG
with DAG(
    'mdt_spatial_analytics_pipeline',
    default_args=default_args,
    description='Pipeline MDT chuẩn Lakehouse (Bỏ qua ML)',
    schedule_interval='@hourly', 
    catchup=False 
) as dag:

    # Tham số chung cho mọi task (Lấy thời gian động của Airflow)
    common_args = [
        '--date', '{{ ds }}', 
        '--hour', '{{ execution_date.strftime("%H") }}'
    ]

    # Task 1: Ingest (Bronze)
    task_ingest = SparkSubmitOperator(
        task_id='1_ingest_to_bronze',
        application='/opt/spark_scripts/1_ingest_data.py',
        application_args=common_args
    )

    # Task 2: Quality Check (Silver & Quarantine) - Tích hợp Gatekeeper ngắt luồng
    task_quality = SparkSubmitOperator(
        task_id='2_run_quality_check',
        application='/opt/spark_scripts/2_quality_check.py',
        application_args=common_args
    )

    # Task 3: H3 Enrichment (Gắn tọa độ)
    task_enrich = SparkSubmitOperator(
        task_id='3_spatial_enrichment_h3',
        application='/opt/spark_scripts/3_h3_enrichment.py',
        application_args=common_args
    )

    # Task 4: Aggregation (Gold)
    task_aggregate = SparkSubmitOperator(
        task_id='4_aggregate_to_gold',
        application='/opt/spark_scripts/4_aggregation.py',
        application_args=common_args
    )

    # Task 5: Đẩy lên Dashboard (Elasticsearch)
    task_push_es = SparkSubmitOperator(
        task_id='5_push_to_es',
        application='/opt/spark_scripts/5_push_to_es.py',
        application_args=common_args
    )

    # 3. KẾT NỐI ĐƯỜNG ỐNG (DEPENDENCIES)
    # Đường ống thẳng tắp từ 1 đến 5. Nếu Task 2 bắn lỗi > 15%, Task 3,4,5 sẽ bị chặn lại (màu cam).
    task_ingest >> task_quality >> task_enrich >> task_aggregate >> task_push_es