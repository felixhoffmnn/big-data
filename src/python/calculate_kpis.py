import argparse
import ast
from os import path

import pyspark

from pyspark.sql.functions import col
from pyspark.sql import SparkSession


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

    for year_month in year_months:
        print("############ {} ############".format(year_month))

        final_file = path.join("/user/hadoop/hubway_data/final/", year_month, "{}-hubway-tripdata.parquet".format(year_month))
        kpi_file = path.join("/user/hadoop/hubway_data/kpis/", "{}-kpis.parquet".format(year_month))

        df = (
            spark.read.format("parquet")
            .options(
                header="true", delimiter=",", nullValue="null", inferschema="true"
            )
            .load(final_file)
        ).dropna(how="any")

        kpis_df = df.agg({"trip_duration": "avg", "trip_distance": "avg"}).withColumnRenamed("avg(trip_duration)", "avg_trip_duration").withColumnRenamed("avg(trip_distance)", "avg_trip_distance")

        print(kpis_df.collect())

        # i = 0
        # for gender in ["Other", "Male", "Female"]:
        #     gender_shares = gender_shares.withColumnRenamed(
        #         f"count(CASE WHEN (gender = {gender}) THEN true END)", gender
        #     )

        #     gender_shares = gender_shares.withColumn(
        #         gender, F.col(gender) / final_data.count()
        #     )

        #     kpis = kpis.withColumn(
        #         f"gender {gender} percentage", lit(f"{gender_shares.collect()[0][i]}")
        #     )
        #     i += 1

        # generation_shares = final_data.agg(
        #     count(when(col("generation") == "Silent Generation", True)),
        #     count(when(col("generation") == "Baby Boomers", True)),
        #     count(when(col("generation") == "Generation X", True)),
        #     count(when(col("generation") == "Millenials", True)),
        #     count(when(col("generation") == "Zoomers", True)),
        #     count(when(col("generation") == "Not given", True)),
        # )

        # i = 0

        # for generation in [
        #     "Silent Generation",
        #     "Baby Boomers",
        #     "Generation X",
        #     "Millenials",
        #     "Zoomers",
        #     "Not given",
        # ]:
        #     generation_shares = generation_shares.withColumnRenamed(
        #         f"count(CASE WHEN (generation = {generation}) THEN true END)", generation
        #     )

        #     generation_shares = generation_shares.withColumn(
        #         generation, F.col(generation) / final_data.count()
        #     )
        #     kpis = kpis.withColumn(
        #         f"generation {generation} percentage",
        #         lit(f"{generation_shares.collect()[0][i]}"),
        #     )
        #     i += 1

        # timeslot_shares = final_data.agg(
        #     count(when(col("timeslot") == 0, True)),
        #     count(when(col("timeslot") == 1, True)),
        #     count(when(col("timeslot") == 2, True)),
        #     count(when(col("timeslot") == 3, True)),
        # )

        # i = 0

        # for timeslot in ["0", "1", "2", "3"]:
        #     timeslot_shares = timeslot_shares.withColumnRenamed(
        #         f"count(CASE WHEN (timeslot = {timeslot}) THEN true END)", timeslot
        #     )

        #     timeslot_shares = timeslot_shares.withColumn(
        #         timeslot, F.col(timeslot) / final_data.count()
        #     )
        #     kpis = kpis.withColumn(
        #         f"timeslot {int(timeslot) * 6} till {int(timeslot) * 6 + 6} percentage",
        #         lit(f"{timeslot_shares.collect()[0][i]}"),
        #     )
        #     i += 1

        print(df.groupBy("bike_id")
        .count()
        .sort(col("count").desc())
        .limit(10)
        .collect())

        print(df.groupBy("start_station_id")
        .count()
        .sort(col("count").desc())
        .limit(10)
        .collect())

        print(df.groupBy("end_station_id")
        .count()
        .sort(col("count").desc())
        .limit(10)
        .collect())

        # Write data to HDFS
        kpis_df.write.format("parquet").mode("overwrite").options(header="true", delimiter=",", nullValue="null", inferschema="true").save(kpi_file)
