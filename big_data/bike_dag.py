"""
Title: Use kaggle.com Hubway Data To Calculate Bike Sharing Usage KPIs
Author: Maxime Fritzsch
Description: BigData Lecture DHBW Stuttgart - Exam 2022/2023
"""
from datetime import datetime
import os, re

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.hdfs_operations import (
    HdfsPutFileOperator,
    HdfsMkdirFileOperator,
)
from airflow.operators.filesystem_operations import (
    ClearDirectoryOperator,
    CreateDirectoryOperator,
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


create_local_import_dir >> clear_local_import_dir


install_kaggle = BashOperator(
    task_id="install_kaggle",
    bash_command="pip install kaggle",
)

download_data = BashOperator(
    task_id="download_data",
    bash_command="kaggle datasets download -d acmeyer/hubway-data --path /home/airflow/bike_data  --unzip",
)


clear_local_import_dir >> install_kaggle >> download_data


raw_mkdir_operator_1 = []
# raw_mkdir_operator_2 = []
raw_put_file_operator_1 = []
# raw_put_file_operator_2 = []

path = "/home/airflow/bike_data/"
files = os.listdir(path)

for file in files:
    if re.search(r"^([0-9]{6})", file):
        year_month = file[:6]

        raw_mkdir_operator_1.append(
            HdfsMkdirFileOperator(
                task_id="mkdir_hdfs_hubway_tripdata_{}".format(year_month),
                directory="/user/hadoop/hubway/raw/{}/".format(year_month),
                hdfs_conn_id="hdfs",
                dag=dag,
            )
        )

        raw_put_file_operator_1.append(
            HdfsPutFileOperator(
                task_id="upload_hubway_to_hdfs_{}".format(year_month),
                local_file=path + file,
                remote_file="/user/hadoop/hubway/raw/{}/{}".format(year_month, file),
                hdfs_conn_id="hdfs",
                dag=dag,
            )
        )

    # elif re.search(r"hubway_Trips_", file):
    #     year = file[:-4][13:]

    #     raw_mkdir_operator_2.append(HdfsMkdirFileOperator(
    #             task_id="mkdir_hdfs_hubway_tripdata_{}".format(year),
    #             directory="/user/hadoop/hubway/raw/per_year/".format(year[:4]),
    #             hdfs_conn_id="hdfs",
    #             dag=dag,
    #         ))


    #     raw_put_file_operator_2.append(HdfsPutFileOperator(
    #             task_id="upload_hubway_to_hdfs_{}".format(year),
    #             local_file=path + file,
    #             remote_file="/user/hadoop/hubway/raw/per_year/{}".format(file),
    #             hdfs_conn_id="hdfs",
    #             dag=dag,
    #         ))

dummy_task_1 = DummyOperator(task_id="dummy_task_1", dag=dag)


for i in range(len(raw_mkdir_operator_1)):
    download_data >> raw_mkdir_operator_1[i] >> raw_put_file_operator_1[i] >> dummy_task_1


# station_mkdir_operator = HdfsMkdirFileOperator(
#         task_id="mkdir_hdfs_hubway_stations",
#         directory="/user/hadoop/hubway/raw/station/",
#         hdfs_conn_id="hdfs",
#         dag=dag,
#     )

# station_put_file_operator = HdfsPutFileOperator(
#         task_id="upload_hubway_stations_to_hdfs",
#         local_file=path + "Hubway_Stations_as_of_July_2017.csv",
#         remote_file="/user/hadoop/hubway/raw/station/stations.csv",
#         hdfs_conn_id="hdfs",
#         dag=dag,
#     )

# pyspark_submit_split_data = SparkSubmitOperator(
#     task_id="pyspark_submit_split_data",
#     conn_id="spark",
#     application="/home/airflow/airflow/python/split_data.py",
#     total_executor_cores='2',
#     executor_cores='2',
#     executor_memory='2g',
#     num_executors='2',
#     name='split_data',
#     verbose=True,
#     dag=dag,
# )

pyspark_submit_raw_data = SparkSubmitOperator(
        task_id="pyspark_submit_raw_data",
        conn_id="spark",
        application="/home/airflow/airflow/python/raw_data.py",
        name='raw_data',
        verbose=True,
        dag=dag,
)


# for i in range(len(raw_mkdir_operator_2)):
#     download_data >> raw_mkdir_operator_2[i] >> raw_put_file_operator_2[i] >> dummy_task_1

# download_data >> station_mkdir_operator >> station_put_file_operator >> dummy_task_1

# dummy_task_1 >> pyspark_submit_split_data
dummy_task_1 >> pyspark_submit_raw_data
