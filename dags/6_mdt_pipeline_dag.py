from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Cấu hình mặc định
default_args = {
    'owner': 'data_engineer_team',
    'depends_on_past': False,
    # Sửa ngày bắt đầu chạy TRÙNG KHỚP với ngày có data đầu tiên
    'start_date': datetime(2026, 5, 5, 0, 0), 
    'end_date': datetime(2026, 5, 5, 23, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Khởi tạo DAG
with DAG(
    'mdt_spatial_analytics_pipeline',
    default_args=default_args,
    description='Pipeline MDT Lakehouse (MinIO -> PostGIS)',
    schedule_interval='@hourly', 
    
    # ĐỔI THÀNH TRUE NHÉ! Đây chính là chìa khóa tự động hóa!
    catchup=True, 
    
    # Khóa luồng, bắt chạy từng giờ một để không nổ RAM
    max_active_runs=1, 
    tags=['mdt']
) as dag:

    # Tham số chung cho mọi task (Lấy thời gian động của Airflow)
    common_args = [
        '--date', '{{ ds }}', 
        '--hour', '{{ execution_date.strftime("%H") }}'
    ]

    # Task 1: Ingest (Landing Zone -> Bronze)
    task_ingest = SparkSubmitOperator(
        task_id='1_ingest_to_bronze',
        application='/opt/spark_scripts/1_ingest_data.py',
        conn_id='spark_default', # Bắt buộc phải có để Airflow trỏ tới Spark Master
        #packages="io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4",
        application_args=common_args
    )

    # Task 2: Quality Check (Silver & Quarantine) - Gatekeeper
    task_quality = SparkSubmitOperator(
        task_id='2_run_quality_check',
        application='/opt/spark_scripts/2_quality_check.py',
        conn_id='spark_default',
        #packages="io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4",
        execution_timeout=timedelta(minutes=10),
        application_args=common_args
    )

    # Task 3: H3 Enrichment (Gắn tọa độ lục giác)
    task_enrich = SparkSubmitOperator(
        task_id='3_spatial_enrichment_h3',
        application='/opt/spark_scripts/3_h3_enrichment.py',
        conn_id='spark_default',
        application_args=common_args
    )

    # Task 4: Aggregation (Gold - Tính KPI)
    task_aggregate = SparkSubmitOperator(
        task_id='4_aggregate_to_gold',
        application='/opt/spark_scripts/4_aggregation.py',
        conn_id='spark_default',
        packages='org.apache.hadoop:hadoop-aws:3.3.4',
        application_args=common_args
    )

    # Task 5: Đẩy lên PostGIS (Tầng Data Warehouse)
    task_push_postgis = SparkSubmitOperator(
        task_id='5_push_to_postgis',
        application='/opt/spark_scripts/5_push_to_postgis.py',
        conn_id='spark_default',
        # Khai báo gói Driver Postgres để Spark tự tải khi submit job
        packages='org.postgresql:postgresql:42.7.3',
        application_args=common_args
    )

    task_visualise = BashOperator(
        task_id='6_visualise',
        # Gọi script python bằng bash thông thường
        bash_command='python /opt/spark_scripts/6_visualise.py --date {{ ds }}'
    )

    # 3. KẾT NỐI ĐƯỜNG ỐNG (DEPENDENCIES)
    # Đường ống thẳng tắp. Nếu Task 2 bắn lỗi văng sys.exit(1), các task sau sẽ ngưng (màu cam).
    task_ingest >> task_quality >> task_enrich >> task_aggregate >> task_push_postgis >> task_visualise