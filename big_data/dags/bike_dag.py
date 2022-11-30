"""
Title: Use kaggle.com Hubway Data To Calculate Bike Sharing Usage KPIs
Author: Maxime Fritzsch
Description: BigData Lecture DHBW Stuttgart - Exam 2022/2023
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


from operators.directory_operator import (
    ClearDirectoryOperator,
    CreateDirectoryOperator,
)
from helpers.year_months import get_year_months
from operators.hdfs_file_operator import (
    HdfsMkdirsFileOperator,
    HdfsPutFilesOperator
)

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

create_local_import_dir = CreateDirectoryOperator(
    task_id="create_import_dir",
    path="/home/airflow",
    directory="bike_data",
    dag=dag,
)

create_output_dir = CreateDirectoryOperator(
    task_id="create_output_dir",
    path="/home/airflow",
    directory="output",
    dag=dag,
)

clear_local_import_dir = ClearDirectoryOperator(
    task_id="clear_import_dir",
    directory="/home/airflow/bike_data",
    pattern="*",
    dag=dag,
)


clear_output_dir = ClearDirectoryOperator(
    task_id="clear_output_dir",
    directory="/home/airflow/output",
    pattern="*",
    dag=dag,
)


download_data = BashOperator(
    task_id="download_data",
    bash_command="kaggle datasets download -d acmeyer/hubway-data --path /home/airflow/bike_data  --unzip",
    dag=dag,
)


downloaded_files = PythonOperator(
    task_id="get_year_months",
    python_callable=get_year_months,
    dag=dag,
)

create_hdfs_hubway_data_partition_dir_raw = HdfsMkdirsFileOperator(
    task_id="mkdirs-hdfs-hubway-data-dir-raw",
    directory="/user/hadoop/hubway_data/raw/",
    file_names=["{{ task_instance.xcom_pull(task_ids='get_year_months') }}"],
    hdfs_conn_id="hdfs",
    dag=dag,
)

create_hdfs_hubway_data_partition_dir_raw.set_upstream(downloaded_files)


create_hdfs_hubway_data_partition_dir_final = HdfsMkdirsFileOperator(
    task_id="mkdirs-hdfs-hubway-data-dir-final",
    directory="/user/hadoop/hubway_data/final/",
    file_names=["{{ task_instance.xcom_pull(task_ids='get_year_months') }}"],
    hdfs_conn_id="hdfs",
    dag=dag,
)

create_hdfs_hubway_data_partition_dir_final.set_upstream(downloaded_files)

create_hdfs_hubway_data_partition_dir_kpis = HdfsMkdirsFileOperator(
    task_id="mkdirs-hdfs-hubway-data-dir-kpis",
    directory="/user/hadoop/hubway_data/kpis/",
    file_names=["{{ task_instance.xcom_pull(task_ids='get_year_months') }}"],
    hdfs_conn_id="hdfs",
    dag=dag,
)

create_hdfs_hubway_data_partition_dir_kpis.set_upstream(downloaded_files)

hdfs_put_hubway_data_raw = HdfsPutFilesOperator(
    task_id="upload-hubway-data-to-hdfs-raw",
    local_path="/home/airflow/bike_data/",
    remote_path="/user/hadoop/hubway_data/raw/",
    file_names=["{{ task_instance.xcom_pull(task_ids='get_year_months') }}"],
    hdfs_conn_id="hdfs",
    dag=dag,
)

pyspark_submit_raw_data = SparkSubmitOperator(
    task_id="pyspark_submit_raw_data",
    conn_id="spark",
    application="/home/airflow/airflow/python/optimize_data.py",
    application_args=["--yearmonth", "{{ task_instance.xcom_pull(task_ids='get_year_months') }}"],
    total_executor_cores=4,
    num_executors=2,
    executor_memory="4G",
    verbose=True,
    dag=dag,
)

pyspark_calculate_kpis = SparkSubmitOperator(
    task_id="pyspark_calculate_kpis",
    conn_id="spark",
    application="/home/airflow/airflow/python/calculate_kpis.py",
    application_args=["--yearmonth", "{{ task_instance.xcom_pull(task_ids='get_year_months') }}"],
    total_executor_cores=4,
    num_executors=2,
    executor_memory="4G",
    verbose=True,
    dag=dag,
)

pyspark_combine_kpis = SparkSubmitOperator(
    task_id="pyspark_combine_kpis",
    conn_id="spark",
    application="/home/airflow/airflow/python/combine_kpis.py",
    application_args=["--yearmonth", "{{ task_instance.xcom_pull(task_ids='get_year_months') }}"],
    total_executor_cores=4,
    num_executors=2,
    executor_memory="4G",
    verbose=True,
    dag=dag,
)


create_local_import_dir >> clear_local_import_dir >> download_data
create_output_dir >> clear_output_dir >> download_data

download_data >> downloaded_files

downloaded_files >> create_hdfs_hubway_data_partition_dir_raw
downloaded_files >> create_hdfs_hubway_data_partition_dir_final
downloaded_files >> create_hdfs_hubway_data_partition_dir_kpis

create_hdfs_hubway_data_partition_dir_raw >> hdfs_put_hubway_data_raw

hdfs_put_hubway_data_raw >> pyspark_submit_raw_data
create_hdfs_hubway_data_partition_dir_final >> pyspark_submit_raw_data

pyspark_submit_raw_data >> pyspark_calculate_kpis
create_hdfs_hubway_data_partition_dir_kpis >> pyspark_calculate_kpis

pyspark_calculate_kpis >> pyspark_combine_kpis
