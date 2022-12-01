import argparse
import ast
from functools import reduce
from os import path

import pyspark
from pyspark.sql import DataFrame, SparkSession


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

    print(year_months)

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    data = []

    # Loop over all year months
    for year_month in year_months:
        print("############ {} ############".format(year_month))

        kpi_file = path.join("/user/hadoop/hubway_data/kpis", year_month, "kpis.parquet")

        # Load the current kpi file and append it to the data list
        data.append(
            spark.read.format("parquet")
            .options(header="true", delimiter=",", nullValue="null", inferschema="true")
            .load(kpi_file)
        )

    # Combine all dataframes into one
    kpi_data = reduce(DataFrame.unionAll, data)

    # Write data to HDFS
    kpi_data.toPandas().to_excel("/home/airflow/output/combined-kpis.xls", index=False)
