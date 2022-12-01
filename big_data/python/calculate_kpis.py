import argparse
import ast
from os import path

import pyspark
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, desc


def get_args():
    parser = argparse.ArgumentParser(
        description="Some Basic Spark Job doing some stuff on hubway data stored within HDFS."
    )
    parser.add_argument("--yearmonth", help="Partion Year Month", required=True, type=str)

    return parser.parse_args()


if __name__ == "__main__":
    """
    Main Function
    """

    # Parse Command Line Args
    args = get_args()
    year_months = ast.literal_eval(args.yearmonth)

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    row = Row(
        "year_month",
        "avg_trip_duration",
        "avg_trip_distance",
        "gender_share",
        "age_share",
        "generation_share",
        "top_used_bikes",
        "top_start_stations",
        "top_end_stations",
        "time_slots",
    )

    # Loop over all year months
    for year_month in year_months:
        print("############ {} ############".format(year_month))

        final_file = path.join("/user/hadoop/hubway_data/final", year_month, "hubway-tripdata.parquet")
        kpi_file = path.join("/user/hadoop/hubway_data/kpis", year_month, "kpis.parquet")

        # Load the current final file
        df = (
            spark.read.format("parquet")
            .options(header="true", delimiter=",", nullValue="null", inferschema="true")
            .load(final_file)
        )

        print(df.collect()[:5])

        # Calculate trip duration, and trip distance
        avg_kpis = (
            df.agg({"trip_duration": "avg", "trip_distance": "avg"})
            .withColumnRenamed("avg(trip_duration)", "avg_trip_duration")
            .withColumnRenamed("avg(trip_distance)", "avg_trip_distance")
            .collect()
        )
        print(avg_kpis[0])
        try:
            avg_trip_distance = float(avg_kpis[0]["avg_trip_distance"])
        except (IndexError, TypeError, ValueError):
            avg_trip_distance = 0.0
        try:
            avg_trip_duration = int(avg_kpis[0]["avg_trip_duration"] / 60)
        except (IndexError, TypeError, ValueError):
            avg_trip_duration = 0

        # Calculate gender share
        gender_df: DataFrame = df.groupBy(col("gender")).count()
        print(gender_df.collect())
        try:
            gender_count_d = gender_df.where(col("gender") == 0).collect()[0]["count"]
        except IndexError:
            gender_count_d = 0
        try:
            gender_count_m = gender_df.where(col("gender") == 1).collect()[0]["count"]
        except IndexError:
            gender_count_m = 0
        try:
            gender_count_f = gender_df.where(col("gender") == 2).collect()[0]["count"]
        except IndexError:
            gender_count_f = 0
        gender_count = gender_count_m + gender_count_f + gender_count_d
        if gender_count > 0:
            m = float(gender_count_m / gender_count * 100)
            w = float(gender_count_f / gender_count * 100)
            d = float(gender_count_d / gender_count * 100)
            # m, w, d
            gender_share = str([("m", m), ("w", w), ("d", d)])
        else:
            gender_share = "[]"

        # Calculate top 10 by age share
        age_share = str(
            [(age, count) for age, count in df.groupBy("age").count().orderBy(desc("count")).collect()][:10]
        )
        generation_share = str(
            [
                (generation, count)
                for generation, count in df.groupBy("generation").count().orderBy(desc("count")).collect()
            ]
        )

        # Calculate top 10 by bike id
        top_used_bikes = str(
            [(id, count) for id, count in df.groupBy("bike_id").count().orderBy(desc("count")).limit(10).collect()]
        )

        # Calculate top 10 by start station
        top_start_stations = str(
            [
                (
                    df.where(col("start_station_id") == id)
                    .select("start_station_name")
                    .limit(1)
                    .collect()[0]["start_station_name"],
                    count,
                )
                for id, count in df.groupBy("start_station_id").count().orderBy(desc("count")).limit(10).collect()
            ]
        )

        # Calculate top 10 by end station
        top_end_stations = str(
            [
                (
                    df.where(col("end_station_id") == id)
                    .select("end_station_name")
                    .limit(1)
                    .collect()[0]["end_station_name"],
                    count,
                )
                for id, count in df.groupBy("end_station_id").count().orderBy(desc("count")).limit(10).collect()
            ]
        )

        # Calculate share of trips by time slot
        try:
            time_slot_0 = df.where(col("timeslot_0") == 1).groupBy("timeslot_0").count().collect()[0]["count"]
        except IndexError:
            time_slot_0 = 0
        try:
            time_slot_1 = df.where(col("timeslot_1") == 1).groupBy("timeslot_1").count().collect()[0]["count"]
        except IndexError:
            time_slot_1 = 0
        try:
            time_slot_2 = df.where(col("timeslot_2") == 1).groupBy("timeslot_2").count().collect()[0]["count"]
        except IndexError:
            time_slot_2 = 0
        try:
            time_slot_3 = df.where(col("timeslot_3") == 1).groupBy("timeslot_3").count().collect()[0]["count"]
        except IndexError:
            time_slot_3 = 0
        time_slots_count = time_slot_0 + time_slot_1 + time_slot_2 + time_slot_3
        if time_slots_count > 0:
            time_slots = str(
                [
                    (0, time_slot_0 / time_slots_count * 100),
                    (1, time_slot_1 / time_slots_count * 100),
                    (2, time_slot_2 / time_slots_count * 100),
                    (3, time_slot_3 / time_slots_count * 100),
                ]
            )
        else:
            time_slots = "[]"

        """
        `year_month`, `avg_trip_duration`, `avg_trip_distance`,
        `gender_share`, `age_share`, `top_used_bikes`,
        `top_start_stations`, `top_end_stations`, `time_slots`
        """

        # Create a new dataframe with the calculated kpis
        kpis_df = spark.createDataFrame(
            [
                row(
                    year_month,
                    avg_trip_duration,
                    avg_trip_distance,
                    gender_share,
                    age_share,
                    generation_share,
                    top_used_bikes,
                    top_start_stations,
                    top_end_stations,
                    time_slots,
                )
            ]
        )

        # Write data to HDFS
        kpis_df.write.format("parquet").mode("overwrite").options(
            header="true", delimiter=",", nullValue="null", inferschema="true"
        ).save(kpi_file)
