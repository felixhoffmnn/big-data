import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import argparse
from pyspark.sql.functions import col, when, count
import pyspark.sql.functions as F


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
    yearmonth = args.yearmonth

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Read Title Basics from HDFS
    final_data = (
        spark.read.format("csv")
        .options(header="true", delimiter=",", nullValue="null", inferschema="true")
        .load(f"/user/hadoop/hubway/final/{yearmonth}/{yearmonth}-hubway-tripdata.csv")
    )

    kpis = spark.createDataFrame(
        [(yearmonth,)],  # create your data here, be consistent in the types.
        ["yearmonth"],  # add your column names here
    )
    avg_dur = final_data.agg({"tripduration": "avg"})
    kpis = kpis.withColumn("avg tripduration", lit(avg_dur.collect()[0][0]))
    avg_dist = final_data.agg({"tripdistance": "avg"})
    kpis = kpis.withColumn("avg tripdistance", lit(avg_dist.collect()[0][0]))

    gender_shares = final_data.agg(
        count(when(col("gender") == "Other", True)),
        count(when(col("gender") == "Male", True)),
        count(when(col("gender") == "Female", True)),
    )

    i = 0
    for gender in ["Other", "Male", "Female"]:
        gender_shares = gender_shares.withColumnRenamed(
            f"count(CASE WHEN (gender = {gender}) THEN true END)", gender
        )

        gender_shares = gender_shares.withColumn(
            gender, F.col(gender) / final_data.count()
        )

        kpis = kpis.withColumn(
            f"gender {gender} percentage", lit(f"{gender_shares.collect()[0][i]}")
        )
        i += 1

    generation_shares = final_data.agg(
        count(when(col("generation") == "Silent Generation", True)),
        count(when(col("generation") == "Baby Boomers", True)),
        count(when(col("generation") == "Generation X", True)),
        count(when(col("generation") == "Millenials", True)),
        count(when(col("generation") == "Zoomers", True)),
        count(when(col("generation") == "Not given", True)),
    )

    i = 0

    for generation in [
        "Silent Generation",
        "Baby Boomers",
        "Generation X",
        "Millenials",
        "Zoomers",
        "Not given",
    ]:
        generation_shares = generation_shares.withColumnRenamed(
            f"count(CASE WHEN (generation = {generation}) THEN true END)", generation
        )

        generation_shares = generation_shares.withColumn(
            generation, F.col(generation) / final_data.count()
        )
        kpis = kpis.withColumn(
            f"generation {generation} percentage",
            lit(f"{generation_shares.collect()[0][i]}"),
        )
        i += 1

    timeslot_shares = final_data.agg(
        count(when(col("timeslot") == 0, True)),
        count(when(col("timeslot") == 1, True)),
        count(when(col("timeslot") == 2, True)),
        count(when(col("timeslot") == 3, True)),
    )

    i = 0

    for timeslot in ["0", "1", "2", "3"]:
        timeslot_shares = timeslot_shares.withColumnRenamed(
            f"count(CASE WHEN (timeslot = {timeslot}) THEN true END)", timeslot
        )

        timeslot_shares = timeslot_shares.withColumn(
            timeslot, F.col(timeslot) / final_data.count()
        )
        kpis = kpis.withColumn(
            f"timeslot {int(timeslot) * 6} till {int(timeslot) * 6 + 6} percentage",
            lit(f"{timeslot_shares.collect()[0][i]}"),
        )
        i += 1

    top_bikes = ""
    i = 1
    for row in (
        final_data.groupBy("bikeid")
        .count()
        .sort(col("count").desc())
        .limit(10)
        .collect()
    ):
        if i == 10:
            top_bikes += f"id{i}: {row['bikeid']}"
        else:
            top_bikes += f"id{i}:{row['bikeid']}, "
        i += 1
    kpis = kpis.withColumn("top_bikes", lit(top_bikes))

    top_start_stationids = ""
    i = 1
    for row in (
        final_data.groupBy("start station id")
        .count()
        .sort(col("count").desc())
        .limit(10)
        .collect()
    ):
        if i == 10:
            top_start_stationids += f"id{i}: {row['start station id']}"
        else:
            top_start_stationids += f"id{i}:{row['start station id']}, "
        i += 1
    top_start_station_names = ""
    i = 1
    for row in (
        final_data.groupBy("start station name")
        .count()
        .sort(col("count").desc())
        .limit(10)
        .collect()
    ):
        if i == 10:
            top_start_station_names += f"name{i}: {row['start station name']}"
        else:
            top_start_station_names += f"name{i}:{row['start station name']}, "
        i += 1

    kpis = kpis.withColumn("top_start_station_ids", lit(top_start_stationids))
    kpis = kpis.withColumn("top_start_station_names", lit(top_start_station_names))

    top_end_stationids = ""
    i = 1
    for row in (
        final_data.groupBy("end station id")
        .count()
        .sort(col("count").desc())
        .limit(10)
        .collect()
    ):
        if i == 10:
            top_end_stationids += f"id{i}: {row['end station id']}"
        else:
            top_end_stationids += f"id{i}:{row['end station id']}, "
        i += 1
    top_end_station_names = ""
    i = 1
    for row in (
        final_data.groupBy("end station name")
        .count()
        .sort(col("count").desc())
        .limit(10)
        .collect()
    ):
        if i == 10:
            top_end_station_names += f"name{i}: {row['end station name']}"
        else:
            top_end_station_names += f"name{i}:{row['end station name']}, "
        i += 1

    kpis = kpis.withColumn("top_end_station_ids", lit(top_end_stationids))
    kpis = kpis.withColumn("top_end_station_names", lit(top_end_station_names))

    # Write data to HDFS
    kpis.write.format("csv").option("header", "true").mode("overwrite").save(
        f"/user/hadoop/hubway/kpis/{yearmonth}-hubway-tripdata.csv"
    )
