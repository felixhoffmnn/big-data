import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, IntegerType, StringType
import argparse
import datetime
from datetime import datetime
from pyspark.sql.functions import date_format
from math import sin, cos, sqrt, atan2, radians
from pyspark.sql.functions import col,when,count
import pyspark.sql.functions as F
import re

def get_args():
    parser = argparse.ArgumentParser(description='Some Basic Spark Job doing some stuff on hubway data stored within HDFS.')
    parser.add_argument('--years', help='years', required=True, type=str)
    parser.add_argument('--months', help='months', required=True, type=str)

    return parser.parse_args()

if __name__ == '__main__':
    """
    Main Function
    """

    # Parse Command Line Args
    args = get_args()
    years = args.years
    months = args.months
    
    years = years.replace("\'", "\"" )
    years = re.findall('"([^"]*)"', years)
    months = months.replace("\'", "\"" )
    months = re.findall('"([^"]*)"', months)
    
    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    data = []
    for year in years:
        for month in months:
            if year != "2016" or month != "12":
                # Read Title Basics from HDFS
                data.append(spark.read.format('csv')\
                    .options(header='true', delimiter=',', nullValue='null', inferschema='true')\
                    .load(f"/user/hadoop/hubway/kpis/{year + month}-hubway-tripdata.csv"))
    
    for i in range(len(data)-1):
        data[i+1] = data[i+1].union(data[i])
    
    kpi_data = data[-1]

    # Write data to HDFS
    kpi_data.write.format('csv').option("header", "true").\
    mode('overwrite').\
    save(f"/user/hadoop/hubway/kpis/overall_kpis.csv")