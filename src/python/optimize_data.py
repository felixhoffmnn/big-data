import argparse
import ast
from math import atan2, cos, radians, sin, sqrt
from os import path

import googlemaps as gmaps
import pyspark
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

from pyspark.sql import SparkSession


def get_args():
    parser = argparse.ArgumentParser(
        description="Some Basic Spark Job doing some stuff on hubway data stored within HDFS."
    )
    parser.add_argument(
        "--yearmonth", help="Partion Year Month", required=True, type=str
    )

    return parser.parse_args()

def get_distance_math (s_lat, s_lon, e_lat, e_lon):
    try:
        distance = R * 2 * atan2(sqrt(sin((radians(e_lat) - radians(s_lat)) / 2) ** 2 + cos(radians(s_lat))* cos(radians(e_lat))
                * sin((radians(e_lon) - radians(s_lon)) / 2) ** 2
            ),
            sqrt(
                1
                - sin((radians(e_lat) - radians(s_lat)) / 2) ** 2
                + cos(radians(s_lat))
                * cos(radians(e_lat))
                * sin((radians(e_lon) - radians(s_lon)) / 2) ** 2
            ),
        )
    except TypeError:
        print("TypeError")
        distance = None

    return distance


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
    client = gmaps.Client(
        queries_per_second=1000, key="AIzaSyBPZubYA_GGZBo8-4eGd5Rfsa9Eui-Sn-o"
    )
    R = 6373.0

    # get_distance = udf(lambda s_lan, s_lon, e_lan, e_lon: client.directions(origin=(s_lan, s_lon), destination=(e_lan, e_lon), units="metric", mode="bicycling")[0]["legs"][0]["distance"]["value"])

    calculate_distance_math = udf(get_distance_math, FloatType())

    for year_month in year_months:
        print("############ {} ############".format(year_month))

        raw_file = path.join("/user/hadoop/hubway_data/raw/", year_month, "{}-hubway-tripdata.csv".format(year_month))
        final_file = path.join("/user/hadoop/hubway_data/final/", year_month, "{}-hubway-tripdata.parquet".format(year_month))

        df = (
            spark.read.format("csv")
            .options(
                header="true", delimiter=",", nullValue="null", inferschema="true"
            )
            .load(raw_file)
        ).dropna(how="any")

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
        )
        
        df_names: DataFrame = (
            df.select(
                "start_station_latitude",
                "start_station_longitude",
                "end_station_latitude",
                "end_station_longitude",
            )
            .groupBy(
                "start_station_latitude",
                "start_station_longitude",
                "end_station_latitude",
                "end_station_longitude",
            )
            .count()
        )

        df_names = df_names.withColumn(
            "trip_distance",
            calculate_distance_math(
                df_names["start_station_latitude"],
                df_names["start_station_longitude"],
                df_names["end_station_latitude"],
                df_names["end_station_longitude"],
            ),
        )

        df = df.join(
            df_names,
            on=[
                "start_station_latitude",
                "start_station_longitude",
                "end_station_latitude",
                "end_station_longitude",
            ],
        )

        df = df.where(df["trip_distance"].isNotNull())

        # Select
        df = df.select(
            "trip_duration",
            "start_time",
            "end_time",
            "start_station_id",
            "end_station_id",
            "start_station_name",
            "end_station_name",
            "bike_id",
            "gender",
            "trip_distance",
        )

        # Write data to HDFS
        df.write.format("parquet").mode("overwrite").options(header="true", delimiter=",", nullValue="null", inferschema="true").save(final_file)
