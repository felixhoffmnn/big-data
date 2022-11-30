import argparse
import ast
from os import path

import pyspark

from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.functions import desc, col, count, sum


def get_args():
    parser = argparse.ArgumentParser(
        description="Some Basic Spark Job doing some stuff on hubway data stored within HDFS."
    )
    parser.add_argument(
        "--yearmonth", help="Partion Year Month", required=True, type=str
    )

    return parser.parse_args()

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

    row = Row(
        "year_month", "avg_trip_duration", "avg_trip_distance",
        "gender_share", "age_share", "top_used_bikes",
        "top_start_stations", "top_end_stations", "time_slots"
    )

    for year_month in year_months:
        print("############ {} ############".format(year_month))

        final_file = path.join("/user/hadoop/hubway_data/final", year_month, "hubway-tripdata.parquet")
        kpi_file = path.join("/user/hadoop/hubway_data/kpis", year_month, "kpis.parquet".format(year_month))

        df = (
            spark.read.format("parquet")
            .options(
                header="true", delimiter=",", nullValue="null", inferschema="true"
            )
            .load(final_file)
        )

        avg_kpis = df.agg({"trip_duration": "avg", "trip_distance": "avg"}).withColumnRenamed("avg(trip_duration)", "avg_trip_duration").withColumnRenamed("avg(trip_distance)", "avg_trip_distance").collect()
        print(avg_kpis[0])
        avg_trip_duration = round(avg_kpis[0]["avg_trip_duration"] / 60, 0)
        avg_trip_distance = avg_kpis[0]["avg_trip_distance"]

        gender_df: DataFrame = df.groupBy(col("gender")).count()
        gender_share_m = gender_df.where(col("gender") == "Male").collect()[0]["count"]
        gender_share_f = gender_df.where(col("gender") == "Female").collect()[0]["count"]
        gender_count = gender_df.agg(sum(col("count"))).withColumnRenamed("sum(count)", "count").collect()[0]["count"]
        m = gender_share_m / gender_count * 100
        w = gender_share_f / gender_count * 100
        d = (gender_count - gender_share_m - gender_share_f) / gender_count * 100
        # m, w, d
        gender_share = [round(m, 2), round(w, 2), round(d, 2)]
        gender_share = str(gender_share)

        # tuple(age, count)
        age_share = str([tuple(element) for element in df.groupBy("age").count().orderBy(desc("count")).collect()])


        # tuple(bikeid, count)
        top_used_bikes = []
        for i in df.groupBy("bike_id").count().orderBy(desc("count")).limit(10).collect():
            top_used_bikes.append(tuple(i))
        top_used_bikes = str(top_used_bikes)

        # tuple(start station id, count)
        top_start_stations = []
        for i in df.groupBy("start_station_id").count().orderBy(desc("count")).limit(10).collect():
            id, count = i
            name = df.where(col("start_station_id") == id).select("start_station_name").limit(1).collect()[0]["start_station_name"]
            top_start_stations.append(tuple((name, count)))
        top_start_stations = str(top_start_stations)


        # tuple(end station id, count)
        top_end_stations = []
        for i in df.groupBy("end_station_id").count().orderBy(desc("count")).limit(10).collect():
            id, count = i
            name = df.where(col("end_station_id") == id).select("end_station_name").limit(1).collect()[0]["end_station_name"]
            top_end_stations.append(tuple((name, count)))
        top_end_stations = str(top_end_stations)


        # tuple(timeslot, percentage)
        time_slots = []
        time_slot_0 = df.where(col("timeslot_0") == 1).groupBy("timeslot_0").count().collect()[0]["count"]
        time_slot_1 = df.where(col("timeslot_1") == 1).groupBy("timeslot_1").count().collect()[0]["count"]
        time_slot_2 = df.where(col("timeslot_2") == 1).groupBy("timeslot_2").count().collect()[0]["count"]
        time_slot_3 = df.where(col("timeslot_3") == 1).groupBy("timeslot_3").count().collect()[0]["count"]
        slot_count = time_slot_0 + time_slot_1 + time_slot_2 + time_slot_3
        time_slots.append(
            tuple((0, round(time_slot_0/slot_count*100, 2)))
        )
        time_slots.append(
            tuple((1, round(time_slot_1/slot_count*100, 2)))
        )
        time_slots.append(
            tuple((2, round(time_slot_2/slot_count*100, 2)))
        )
        time_slots.append(
            tuple((3, round(time_slot_3/slot_count*100, 2)))
        )
        time_slots = str(time_slots)

        """
        `year_month`, `avg_trip_duration`, `avg_trip_distance`,
        `gender_share`, `age_share`, `top_used_bikes`,
        `top_start_stations`, `top_end_stations`, `time_slots`
        """

        kpis_df = spark.createDataFrame([row(year_month, avg_trip_duration, avg_trip_distance, gender_share, age_share, top_used_bikes, top_start_stations, top_end_stations, time_slots)])

        # Write data to HDFS
        kpis_df.write.format("parquet").mode("overwrite").options(header="true", delimiter=",", nullValue="null", inferschema="true").save(kpi_file)
