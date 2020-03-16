# Spark EMR ETL
# 1) read in staging tables from csvs
#   a) match_data
#   b) match_player_data
#   c) match_skill_data
# 2) parse out additional start_time table
# 3) save to S3 as parquet format

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType

# read from config
config = configparser.ConfigParser()
config.read('secret.cfg')
[KEY, SECRET] = config['AWS'].values()

# set env vars
os.environ['AWS_ACCESS_KEY_ID']=KEY
os.environ['AWS_SECRET_ACCESS_KEY']=SECRET

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark

def process_data(spark, match_data, match_player_data, match_skill_data, output_data):
    '''
    This function reads in data from 3 csvs into 3 SQL staging tables, transforms, then saves as parquet
    '''
    # read match_data
    df_match_data = spark.read.csv(match_data)
    df_match_data.printSchema()

    # drop unused columns
    drop_cols = ['match_seq_num', 'positive_votes', 'negative_votes', 'parse_status', 'pgroup']
    df_match_data = df_match_data.drop(*drop_cols)

    # write slimmed dataframe
    print(f'writing {output_data}match_data_table')
    df_match_data.write.mode('overwrite').parquet(f'{output_data}match_data_table')

    # create custom time columns
    @udf(TimestampType())
    def get_timestamp(x):
        return datetime.fromtimestamp(x)

    df_match_data = df_match_data.select('start_time').withColumn('timestamp', get_timestamp('start_time'))
    df_match_data = df_match_data.withColumn('datetime', col('timestamp').cast(DateType()))

    time_table = df_match_data.selectExpr("timestamp as start_time")
    time_table = time_table.withColumn('hour', hour('start_time')) \
        .withColumn('day', dayofmonth('start_time')) \
        .withColumn('week', weekofyear('start_time')) \
        .withColumn('month', month('start_time')) \
        .withColumn('year', year('start_time')) \
        .withColumn('weekday', date_format('start_time', 'u'))
    
    # write time table to parquet files partitioned by year and month
    print(f'writing {output_data}time_table')
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(f'{output_data}time_table')

def main():
    # if running in test mode, use tiny files instead of full size
    test_mode = True
    test_suffix = ''

    if test_mode:
        print('##### RUNNING IN TEST MODE WITH SMALL FILES #####')
        test_suffix = '-test'
    else:
        print('##### RUNNING IN LIVE MODE WITH FULL FILES #####')

    # https://s3.us-west-2.amazonaws.com/mybucket/puppy.jpg
    match_data = f's3a://jf-dend-capstone/open-dota{test_suffix}/matches_small{test_suffix}.csv'
    match_player_data = f's3a://jf-dend-capstone/open-dota{test_suffix}/player_matches_small{test_suffix}.csv'
    match_skill_data = f's3a://jf-dend-capstone/open-dota{test_suffix}/match_skill{test_suffix}.csv'
    output_data = "s3a://udacity-dend/"

    print(f'match_data path: {match_data}')
    print(f'match_player_data path: {match_player_data}')
    print(f'match_skill_data path: {match_skill_data}')

    spark = create_spark_session()

    process_data(spark, match_data, match_player_data, match_skill_data, output_data)

if __name__ == "__main__":
    main()