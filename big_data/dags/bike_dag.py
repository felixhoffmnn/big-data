"""
Title: Use kaggle.com Hubway Data To Calculate Bike Sharing Usage KPIs
Author: Maxime Fritzsch
Description: BigData Lecture DHBW Stuttgart - Exam 2022/2023
"""
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import HdfsMkdirsFileOperator, HdfsPutFilesOperator, ClearDirectoryOperator, CreateDirectoryOperator
from airflow.operators.python_operator import PythonOperator
from helpers.year_months import get_year_months

args = {"owner": "airflow"}

dag = DAG(
    "bike_dag",
    default_args=args,
    description="BigData Lecture DHBW Stuttgart - Exam 2022/2023",
    schedule_interval="56 18 * * *",
    start_date=datetime(2019, 10, 16),
    catchup=False,
    max_active_runs=1,
)

# Create download directory if not exists
create_local_import_dir = CreateDirectoryOperator(
    task_id="create_import_dir",
    path="/home/airflow",
    directory="bike_data",
    dag=dag,
)

# Create output directory if not exists
create_output_dir = CreateDirectoryOperator(
    task_id="create_output_dir",
    path="/home/airflow",
    directory="output",
    dag=dag,
)

# Clear import directory for clean run
clear_local_import_dir = ClearDirectoryOperator(
    task_id="clear_import_dir",
    directory="/home/airflow/bike_data",
    pattern="*",
    dag=dag,
)

# Clear output directory
clear_output_dir = ClearDirectoryOperator(
    task_id="clear_output_dir",
    directory="/home/airflow/output",
    pattern="*",
    dag=dag,
)

# Download data from kaggle.com
download_data = BashOperator(
    task_id="download-data",
    bash_command="kaggle datasets download -d acmeyer/hubway-data --path /home/airflow/bike_data  --unzip",
    dag=dag,
)

# Collect a list of all files in the import directory
list_files = PythonOperator(
    task_id="get-year-months",
    python_callable=get_year_months,
    dag=dag,
)

# Create partition directories in HDFS for raw data
create_hdfs_partition_raw = HdfsMkdirsFileOperator(
    task_id="mkdirs-hdfs-raw",
    directory="/user/hadoop/hubway_data/raw/",
    file_names=["{{ task_instance.xcom_pull(task_ids='get-year-months') }}"],
    hdfs_conn_id="hdfs",
    dag=dag,
)

create_hdfs_partition_raw.set_upstream(list_files)

# Create partition directories in HDFS for processed data
create_hdfs_partition_final = HdfsMkdirsFileOperator(
    task_id="mkdirs-hdfs-final",
    directory="/user/hadoop/hubway_data/final/",
    file_names=["{{ task_instance.xcom_pull(task_ids='get-year-months') }}"],
    hdfs_conn_id="hdfs",
    dag=dag,
)

create_hdfs_partition_final.set_upstream(list_files)

# Create partition directories in HDFS for calculated kpis
create_hdfs_partition_kpis = HdfsMkdirsFileOperator(
    task_id="mkdirs-hdfs-kpis",
    directory="/user/hadoop/hubway_data/kpis/",
    file_names=["{{ task_instance.xcom_pull(task_ids='get-year-months') }}"],
    hdfs_conn_id="hdfs",
    dag=dag,
)

create_hdfs_partition_kpis.set_upstream(list_files)

# Upload raw data to HDFS
copy_raw_to_hdfs = HdfsPutFilesOperator(
    task_id="upload-raw-to-hdfs",
    local_path="/home/airflow/bike_data/",
    remote_path="/user/hadoop/hubway_data/raw/",
    file_names=["{{ task_instance.xcom_pull(task_ids='get-year-months') }}"],
    hdfs_conn_id="hdfs",
    dag=dag,
)

# Process raw data
pyspark_process_raw = SparkSubmitOperator(
    task_id="pyspark-process-raw",
    conn_id="spark",
    application="/home/airflow/airflow/python/optimize_data.py",
    application_args=["--yearmonth", "{{ task_instance.xcom_pull(task_ids='get-year-months') }}"],
    total_executor_cores=4,
    num_executors=2,
    executor_memory="4G",
    verbose=True,
    dag=dag,
)

# Calculate KPIs
pyspark_calculate_kpis = SparkSubmitOperator(
    task_id="pyspark_calculate_kpis",
    conn_id="spark",
    application="/home/airflow/airflow/python/calculate_kpis.py",
    application_args=["--yearmonth", "{{ task_instance.xcom_pull(task_ids='get-year-months') }}"],
    total_executor_cores=4,
    num_executors=2,
    executor_memory="4G",
    verbose=True,
    dag=dag,
)

# Combine calculated KPIs
pyspark_combine_kpis = SparkSubmitOperator(
    task_id="pyspark_combine_kpis",
    conn_id="spark",
    application="/home/airflow/airflow/python/combine_kpis.py",
    application_args=["--yearmonth", "{{ task_instance.xcom_pull(task_ids='get-year-months') }}"],
    total_executor_cores=4,
    num_executors=2,
    executor_memory="4G",
    verbose=True,
    dag=dag,
)


create_local_import_dir >> clear_local_import_dir >> download_data
create_output_dir >> clear_output_dir >> download_data

download_data >> list_files

list_files >> create_hdfs_partition_raw
list_files >> create_hdfs_partition_final
list_files >> create_hdfs_partition_kpis

create_hdfs_partition_raw >> copy_raw_to_hdfs

copy_raw_to_hdfs >> pyspark_process_raw
create_hdfs_partition_final >> pyspark_process_raw

pyspark_process_raw >> pyspark_calculate_kpis
create_hdfs_partition_kpis >> pyspark_calculate_kpis

pyspark_calculate_kpis >> pyspark_combine_kpis
