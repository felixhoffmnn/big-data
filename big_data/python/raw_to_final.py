from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, IntegerType, StringType
import argparse
import datetime
from datetime import datetime
from pyspark.sql.functions import date_format
from math import sin, cos, sqrt, atan2, radians

def get_args():
    parser = argparse.ArgumentParser(description='Some Basic Spark Job doing some stuff on hubway data stored within HDFS.')
    parser.add_argument('--yearmonth', help='Partion Year Month', required=True, type=str)

    return parser.parse_args()

if __name__ == '__main__':
    """
    Main Function
    """

    # Parse Command Line Args
    args = get_args()
    
    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)
    
    # Read Title Basics from HDFS
    raw_data = spark.read.format('csv')\
        .options(header='true', delimiter=',', nullValue='null', inferschema='true')\
        .load(f"/user/hadoop/hubway/raw/{args.yearmonth}/{args.yearmonth}-hubway-tripdata.csv")
        
    yearmonth = args.yearmonth
        
    def calculate_distance(lat1, lat2, lon1, lon2):
        try:
            lat1 = float(lat1)
            lon1 = float(lon1)
            lat2 = float(lat2)
            lon2 = float(lon2)
            lat1 = radians(lat1)
            lon1 = radians(lon1)
            lat2 = radians(lat2)
            lon2 = radians(lon2)
            R = 6373.0
            dlon = lon2 - lon1
            dlat = lat2 - lat1

            a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))
            return R * c
        except TypeError:
            return 0
        except ValueError:
            return 0

    def calculate_duration(starttime, stoptime):
        duration = (stoptime - starttime).total_seconds() / 60.0
        return duration

    def calculate_generation(birth_year):
        try:
            if (int(birth_year) < 1945):
                return 'Silent Generation'
            elif (int(birth_year) < 1964):
                return 'Baby Boomers'
            elif (int(birth_year) < 1980):
                return 'Generation X'
            elif (int(birth_year) < 1996):
                return 'Millenials'
            elif (int(birth_year) < 2012):
                return 'Zoomers'
            else:
                return 'Not given'
        except ValueError:
            return "Not given"
        

    def calculate_timeslot(starttime, stoptime):
        time1 = starttime.replace(hour=0, minute=0, second=0)
        time2 = starttime.replace(hour=6, minute=0, second=0)
        time3 = starttime.replace(hour=12, minute=0, second=0)
        time4 = starttime.replace(hour=18, minute=0, second=0)
        time5 = starttime.replace(hour=23, minute=59, second=59)
        
        if (starttime < time2):
            if (stoptime < time2):
                return 0
            else:
                if (((stoptime - time2).total_seconds() / 60.0) > ((time2 - starttime).total_seconds() / 60.0)):
                    return 1
                else:
                    return 0
        if (starttime < time3):
            if (stoptime < time3):
                return 1
            else:
                if (((stoptime - time3).total_seconds() / 60.0) > ((time3 - starttime).total_seconds() / 60.0)):
                    return 2
                else:
                    return 1
        if (starttime < time4):
            if (stoptime < time4):
                return 2
            else:
                if (((stoptime - time4).total_seconds() / 60.0) > ((time4 - starttime).total_seconds() / 60.0)):
                    return 3
                else:
                    return 2
        if (starttime <= time5):
            if (stoptime <= time5):
                return 3
            else:
                if (((stoptime - time1).total_seconds() / 60.0) > ((time1 - starttime).total_seconds() / 60.0)):
                    return 0
                else:
                    return 3

    def change_gender(gender):
        if gender == 0:
            return "Other"
        if gender == 1:
            return "Male"
        if gender == 2:
            return "Female"

    udf_calc_dist = udf(calculate_distance, FloatType()) 

    raw_data = raw_data.withColumn('tripdistance',
                    udf_calc_dist(
                            'start station latitude',  
                            'end station latitude',
                            'start station longitude',
                            'end station longitude'
                        )
                    )

    udf_calc_gend = udf(change_gender, StringType()) 

    raw_data = raw_data.withColumn('gender',
                    udf_calc_gend(
                            'gender'
                        )
                    )

    udf_calc_generation = udf(calculate_generation, StringType()) 

    raw_data = raw_data.withColumn('generation',udf_calc_generation('birth year'))

    udf_calc_duration = udf(calculate_duration, FloatType()) 

    raw_data = raw_data.withColumn('tripduration',
                    udf_calc_duration(
                            'starttime',  
                            'stoptime'
                        )
                    )
    udf_calc_timeslot = udf(calculate_timeslot, IntegerType()) 

    raw_data = raw_data.withColumn('timeslot',
                    udf_calc_timeslot(
                            'starttime',  
                            'stoptime'
                        )
                    )



    raw_data = raw_data.drop('usertype', 'starttime', 'stoptime', 'birth year', 'start station latitude', 'end station latitude', 'start station longitude', 'end station longitude')

    # Write data to HDFS
    raw_data.write.format('csv').option("header", "true").\
            mode('overwrite').\
        save(f"/user/hadoop/hubway/final/{yearmonth}/{yearmonth}-hubway-tripdata.csv")