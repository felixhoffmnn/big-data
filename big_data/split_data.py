import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
import googlemaps


if __name__ == '__main__':
    """
    Main Function
    """
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)
    gmaps = googlemaps.Client(key="AIzaSyBPZubYA_GGZBo8-4eGd5Rfsa9Eui-Sn-o")

    path = "/user/hadoop/hubway/raw/per_year/"
    files = ["hubway_Trips_2011.csv", "hubway_Trips_2012.csv", "hubway_Trips_2013.csv", "hubway_Trips_2014_1.csv", "hubway_Trips_2014_2.csv"]

    for file in files:
        year = file[13:17]
        
        df = spark.read.format("csv").options(header="true", delimiter=",", nullValue="null", inferschema="true").load(path + file)
        
        df = df.withColumnRenamed("Duration", "trip_duration").withColumnRenamed("Start date", "start_time").withColumnRenamed("End date", "end_time").withColumnRenamed("Start station number", "start_station_id").withColumnRenamed("Start station name", "start_station_name").withColumnRenamed("End station number", "end_station_id").withColumnRenamed("End station name", "end_station_name").withColumnRenamed("Bike number", "bike_id").withColumnRenamed("Member type", "member_type").withColumnRenamed("Zip Code", "zip_code").withColumnRenamed("Gender", "gender")
        
        reg_trip_duration = udf(lambda x: int(x) / 1000, IntegerType())
        
        df = df.withColumn("trip_duration", reg_trip_duration(col("trip_duration")))
        
        # TODO: Calculate Distance
        df = df.withColumn("trip_distance", pass)
        
        df = df.withColumn("formatted_time", col("start_time").substr(1, 2))
        
        for m in range (1, 13):
            current_month = df.where(df.formatted_time == m)
            
            current_month = current_month.select("trip_duration", "trip_distance", "start_time", "end_time", "start_station_id", "end_station_id", "start_station_name", "end_station_name", "bike_id", "gender")
            
            if len(current_month.head(1)) > 0:
                current_month.write.format("csv").mode("overwrite").save("/user/hadoop/hubway/final/{}{}/{}{}-hubway-tripdata.csv".format(year, m, year, m))
        