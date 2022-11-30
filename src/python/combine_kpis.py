import argparse
from functools import reduce
import ast
from os import path

import pyspark
from pyspark.sql import SparkSession, DataFrame


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

    data = []
    for year_month in year_months:
        print("############ {} ############".format(year_month))

        kpi_file = path.join("/user/hadoop/hubway_data/kpis/", "{}-kpis.parquet".format(year_month))

        data.append((
            spark.read.format("parquet")
            .options(
                header="true", delimiter=",", nullValue="null", inferschema="true"
            )
            .load(kpi_file)
        ).dropna(how="any"))

    kpi_data = reduce(DataFrame.unionAll, data)

    # Write data to HDFS
    # TODO: Export to Excel
    kpi_data.toPandas().to_csv("/home/airflow/excel_files/combined-kpis.csv", index=False)
