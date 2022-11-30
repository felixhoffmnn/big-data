import argparse
import ast
from datetime import datetime
from math import atan2, cos, radians, sin, sqrt
from os import path

# import googlemaps as gmaps

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col
import pyspark
from pyspark.sql.types import IntegerType, FloatType



def get_args():
    parser = argparse.ArgumentParser(
        description="Some Basic Spark Job doing some stuff on hubway data stored within HDFS."
    )
    parser.add_argument(
        "--yearmonth", help="Partion Year Month", required=True, type=str
    )

    return parser.parse_args()

# https://stackoverflow.com/questions/365826/calculate-distance-between-2-gps-coordinates
def get_distance (s_lat, s_lon, e_lat, e_lon):
    R = 6373.0

    try:
        lat_1 = float(s_lat)
        lon_1 = float(s_lon)
        lat_2 = float(e_lat)
        lon_2 = float(e_lon)

        delta_lat = radians(lat_2 - lat_1)
        delta_lon = radians(lon_2 - lon_1)

        radian_lat_1 = radians(lat_1)
        radian_lat_2 = radians(lat_2)

        a = sin(delta_lat / 2)**2 + cos(radian_lat_1) * cos(radian_lat_2) * sin(delta_lon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        distance = round(R * c, 2)
    except TypeError:
        print("TypeError")
        distance = 0.0
    except ValueError:
        print("ValueError")
        distance = 0.0

    return distance

def get_age(birth_year):
    try:
        age = datetime.now().year - int(birth_year)
    except TypeError:
        print("TypeError")
        age = 0
    except ValueError:
        print("ValueError")
        age = 0

    return age

def get_timeslot_helper(time: datetime):
    if (0 <= time.hour < 6):
        return 0
    elif (6 <= time.hour < 12):
        return 1
    elif (12 <= time.hour < 18):
        return 2
    elif (19 <= time.hour <= 24):
        return 3

    return -1

def get_timeslot(start_time: datetime, end_time: datetime, time_slot: int):
    start_slot = get_timeslot_helper(start_time)
    end_slot = get_timeslot_helper(end_time)

    if start_slot or end_slot == -1:
        return -1

    if start_slot == time_slot or end_slot == time_slot:
        return 1
    if start_slot < time_slot and end_slot > time_slot:
        return 1
    if start_slot > time_slot and end_slot > time_slot and start_slot < end_slot:
        return 1

    return 0

def get_generation(birth_year):
    try:
        year = int(birth_year)

        if (year < 1945):
            return 0
        elif (year < 1964):
            return 1
        elif (year < 1980):
            return 2
        elif (year < 1996):
            return 3
        elif (year < 2012):
            return 4
        else:
            return 5
    except ValueError:
        return -1
    except TypeError:
        return -1

def convert_seconds_to_minutes(duration):
    try:
        duration = int(duration)
        duration = duration / 60
    except ValueError:
        duration = 0
    except TypeError:
        duration = 0

    return duration


if __name__ == "__main__":
    """
    Main Function
    """

    # Parse Command Line Args
    args = get_args()
    year_months = ast.literal_eval(args.yearmonth)

    print(year_months)

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)
    # client = gmaps.Client(
    #     queries_per_second=1000, key="AIzaSyBPZubYA_GGZBo8-4eGd5Rfsa9Eui-Sn-o"
    # )

    get_distance_udf = udf(get_distance, FloatType())
    get_age_udf = udf(get_age, IntegerType())
    get_timeslot_udf = udf(get_timeslot, IntegerType())
    convert_seconds_to_minutes_udf = udf(convert_seconds_to_minutes, IntegerType())
    get_generation_udf = udf(get_generation, IntegerType())


    for year_month in year_months:
        print("############ {} ############".format(year_month))

        raw_file = path.join("/user/hadoop/hubway_data/raw", year_month, "{}-hubway-tripdata.csv".format(year_month))
        final_file = path.join("/user/hadoop/hubway_data/final", year_month, "hubway-tripdata.parquet")

        df = (
            spark.read.format("csv")
            .options(
                header="true", delimiter=",", nullValue="null", inferschema="true"
            )
            .load(raw_file)
        )
        df = (
            df.withColumnRenamed("tripduration", "trip_duration")
            .withColumnRenamed("starttime", "start_time")
            .withColumnRenamed("stoptime", "end_time")
            .withColumnRenamed("start station id", "start_station_id")
            .withColumnRenamed("start station name", "start_station_name")
            .withColumnRenamed("start station latitude", "start_station_latitude")
            .withColumnRenamed("start station longitude", "start_station_longitude")
            .withColumnRenamed("end station id", "end_station_id")
            .withColumnRenamed("end station name", "end_station_name")
            .withColumnRenamed("end station latitude", "end_station_latitude")
            .withColumnRenamed("end station longitude", "end_station_longitude")
            .withColumnRenamed("bikeid", "bike_id")
            .withColumnRenamed("usertype", "user_type")
            .withColumnRenamed("birth year", "birth_year")
        ).dropna(how="any").where((col("trip_duration") > 0) & (col("trip_duration") < 1440))

        # Create smaller Dataframe to reduce calculations or api requests
        df_distance: DataFrame = (
            df.groupBy(
                "start_station_latitude",
                "start_station_longitude",
                "end_station_latitude",
                "end_station_longitude",
            ).count()
        )
        df_distance = df_distance.withColumn(  # type: ignore
            "trip_distance",
            get_distance_udf(
                col("start_station_latitude"),
                col("start_station_longitude"),
                col("end_station_latitude"),
                col("end_station_longitude"),
            ),
        )
        df = df.join(
            df_distance.select(
                "start_station_latitude",
                "start_station_longitude",
                "end_station_latitude",
                "end_station_longitude",
                "trip_distance",
            ),
            on=[
                "start_station_latitude",
                "start_station_longitude",
                "end_station_latitude",
                "end_station_longitude",
            ],
        )

        df = df.withColumn("trip_duration", convert_seconds_to_minutes_udf(col("trip_duration")))
        df = df.withColumn("generation", get_generation_udf(col("birth_year")))
        df = df.withColumn("age", get_age_udf(col("birth_year")))
        for i in range(4):
            df = df.withColumn(
                "timeslot_{}".format(i),
                get_timeslot_udf(
                    col("start_time"),
                    col("end_time"),
                    i,  # type: ignore
                )
            )

        # Select
        df = df.where((col("trip_distance") > 0) & (col("age") > 0) & (col("generation") >= 0))
        df = df.drop(
            "start_time",
            "end_time",
            "start_station_latitude",
            "start_station_longitude",
            "end_station_latitude",
            "end_station_longitude",
            "user_type",
            "birth_year",
        )

        # Write data to HDFS
        df.write.format("parquet").mode("overwrite").options(header="true", delimiter=",", nullValue="null", inferschema="true").save(final_file)
