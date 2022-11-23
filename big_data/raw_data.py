import os
import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
import googlemaps as gmaps 

# def get_items(path: str):
#     p = subprocess.Popen("hdfs dfs -ls {}".format(path) + " |  awk '{print $8}'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

#     l = [str(line).replace("\\n", "").replace("\\N", "").replace("b'", "").replace("'", "") for line in p.stdout if str(line) != "b'\\n'" or str(line) != "b'\\N'"]

#     return l


if __name__ == '__main__':
    """
    Main Function
    """ 

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)
    client = gmaps.Client(queries_per_second=1000, key="AIzaSyBPZubYA_GGZBo8-4eGd5Rfsa9Eui-Sn-o")
    
    get_distance = udf(lambda s_lan, s_lon, e_lan, e_lon: client.directions(origin=(s_lan, s_lon), destination=(e_lan, e_lon), units="metric", mode="bicycling")[0]["legs"][0]["distance"]["value"])
    
    for file in os.listdir("/home/airflow/bike_data/"):
        if re.search(r"^([0-9]{6})", file):
            print("############ {} ############".format(file))
            
            year_month = file[:6]
            
            df = spark.read.format('csv').options(header='true', delimiter=',', nullValue='null', inferschema='true').load("/user/hadoop/hubway/raw/{}/{}".format(year_month, file))
                
            df = df.withColumnRenamed("tripduration", "trip_duration").withColumnRenamed("starttime", "start_time").withColumnRenamed("stoptime", "end_time").withColumnRenamed("start station id", "start_station_id").withColumnRenamed("start station name", "start_station_name").withColumnRenamed("start station latitude", "start_station_latitude").withColumnRenamed("start station longitude", "start_station_longitude").withColumnRenamed("end station id", "end_station_id").withColumnRenamed("end station name", "end_station_name").withColumnRenamed("end station latitude", "end_station_latitude").withColumnRenamed("end station longitude", "end_station_longitude").withColumnRenamed("bikeid", "bike_id").withColumnRenamed("usertype", "user_type").withColumnRenamed("birth year", "birth_year")

            df_names = df.select("start_station_latitude", "start_station_longitude", "end_station_latitude", "end_station_longitude").groupBy("start_station_latitude", "start_station_longitude", "end_station_latitude", "end_station_longitude").count()
            
            df_names = df_names.where((col("start_station_latitude") != 0.0) & (col("start_station_longitude") != 0.0) & (col("end_station_latitude") != 0.0) & (col("end_station_longitude") != 0.0))
            
            df_names = df_names.withColumn("trip_distance", get_distance(col("start_station_latitude"), col("start_station_longitude"), col("end_station_latitude"), col("end_station_longitude")))
            
            df = df.join(df_names, on=["start_station_latitude", "start_station_longitude", "end_station_latitude", "end_station_longitude"])

            # Select
            df = df.select("trip_duration", "start_time", "end_time", "start_station_id", "end_station_id", "start_station_name", "end_station_name", "bike_id", "gender", "trip_distance")

            # Write data to HDFS
            df.write.format('csv').mode('overwrite').save("/user/hadoop/hubway/final/{}/{}".format(year_month, file))