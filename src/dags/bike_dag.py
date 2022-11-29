"""
Title: Use kaggle.com Hubway Data To Calculate Bike Sharing Usage KPIs
Author: Maxime Fritzsch
Description: BigData Lecture DHBW Stuttgart - Exam 2022/2023
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from operators.directory_operator import (
    ClearDirectoryOperator,
    CreateDirectoryOperator,
)
from operators.hdfs_file_operator import (
    HdfsMkdirFileOperator,
    HdfsMkdirsFileOperator,
    HdfsPutFilesOperator,
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

clear_local_import_dir = ClearDirectoryOperator(
    task_id="clear_import_dir",
    directory="/home/airflow/bike_data",
    pattern="*",
    dag=dag,
)

# TODO: Move to helper
def get_year_month():
    files = [
        file for file in os.listdir("/home/airflow/bike_data") if file[:6].isdigit()
    ]
    return [file[:6] for file in files]


download_data = BashOperator(
    task_id="download_data",
    bash_command="kaggle datasets download -d acmeyer/hubway-data --path /home/airflow/bike_data  --unzip",
    dag=dag,
)


downloaded_files = PythonOperator(
    task_id="get_year_month",
    python_callable=get_year_month,
    dag=dag,
)

create_hdfs_hubway_data_partition_dir_raw = HdfsMkdirsFileOperator(
    task_id="mkdir-hdfs-hubway-data-dir-raw",
    directory="/user/hadoop/hubway_data/raw/",
    file_names=["{{ task_instance.xcom_pull(task_ids='get_year_month') }}"],
    hdfs_conn_id="hdfs",
    dag=dag,
)

create_hdfs_hubway_data_partition_dir_raw.set_upstream(downloaded_files)


create_hdfs_hubway_data_partition_dir_final = HdfsMkdirsFileOperator(
    task_id="mkdir-hdfs-hubway-data-dir-final",
    directory="/user/hadoop/hubway_data/final/",
    file_names=["{{ task_instance.xcom_pull(task_ids='get_year_month') }}"],
    hdfs_conn_id="hdfs",
    dag=dag,
)

create_hdfs_hubway_data_partition_dir_final.set_upstream(downloaded_files)

create_hdfs_hubway_data_partition_dir_hiveSQL = HdfsMkdirFileOperator(
    task_id="mkdir-hdfs-hubway-data-dir-hiveSQL",
    directory="/user/hadoop/hubway_data/hiveSQL/",
    hdfs_conn_id="hdfs",
    dag=dag,
)

create_hdfs_hubway_data_partition_dir_kpis = HdfsMkdirFileOperator(
    task_id="mkdir-hdfs-hubway-data-dir-kpis",
    directory="/user/hadoop/hubway_data/kpis/",
    hdfs_conn_id="hdfs",
    dag=dag,
)

hdfs_put_hubway_data_raw = HdfsPutFilesOperator(
    task_id="upload-hubway-data-to-hdfs-raw",
    local_path="/home/airflow/hubway_data/",
    remote_path="/user/hadoop/hubway_data/raw/",
    file_names=["{{ task_instance.xcom_pull(task_ids='get_year_month') }}"],
    hdfs_conn_id="hdfs",
    dag=dag,
)


create_local_import_dir >> clear_local_import_dir

clear_local_import_dir >> download_data >> downloaded_files

downloaded_files >> create_hdfs_hubway_data_partition_dir_raw
downloaded_files >> create_hdfs_hubway_data_partition_dir_final
downloaded_files >> create_hdfs_hubway_data_partition_dir_hiveSQL
downloaded_files >> create_hdfs_hubway_data_partition_dir_kpis

create_hdfs_hubway_data_partition_dir_raw >> hdfs_put_hubway_data_raw
