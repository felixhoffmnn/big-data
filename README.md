# Big Data

## Initial Setup

```bash
docker exec -it hadoop bash
sudo su hadoop
cd
start-all.sh
hiveserver2

mkdir /home/airflow/.kaggle
sudo vi /home/airflow/.kaggle/kaggle.json

docker exec -it airflow bash
sudo rm /home/airflow/airflow/dags/testing_dag.py
sudo vi /home/airflow/airflow/dags/testing_dag.py
sudo vi /home/airflow/airflow/python/pyspark_raw_to_final.py
```
# big_data
