"""


"""
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, LongType, IntegerType
import tqdm
import os

import config as app_cfg

config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Helper function to fetch the SparkSession
    
    Returns
    -------
    spark: pyspark.sql.session.SparkSession
        Creates or fetches an existing SparkSession object; the entry point to programming Spark.
    """
    extra_jars = []
    if not app_cfg.APP_DEV:
        extra_jars.append("org.apache.hadoop:hadoop-aws:2.7.0")
    
    spark = SparkSession \
        .builder \
        .appName("DataLakeSpark") \
        .config("spark.jars.packages", extra_jars) \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Executes a basic EL pipeline, preparing the dimension tables.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        A existing SparkSession object; the entry point to programming Spark.

    input_data: string
        Source directory from where the data will be read.

    output_data: string
        Target output directory; the Parquet will be written there.
    """
    # get filepath to song data file
    song_data = f"{input_data}/song_data/*/*/*/*.json"

    # A bit of generic schema
    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", IntegerType())
    ])

    # read song data file
    df = spark.read.json(song_data, schema=song_schema)

    # # extract columns to create songs table
    columns = ["song_id", "title", "artist_id", "year", "duration" ]
    songs_table = df.select(*columns).dropDuplicates(["song_id"])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(f"{output_data}/songs_table/")

    # extract columns to create artists table
    columns = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]
    artists_table = df.selectExpr(*columns).dropDuplicates(["artist_id"])

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(f"{output_data}/artists_table/")

def process_log_data(spark, input_data, output_data):
    """ Creates the fact table `songplays`.

    Parameters
    ----------
    spark: pyspark.sql.session.SparkSession
        A existing SparkSession object; the entry point to programming Spark.

    input_data: string
        Source directory from where the data will be read.

    output_data: string
        Target output directory; the Parquet will be written there.
    """
    # get filepath to log data file
    log_data = f"{input_data}/log_data/*.json"

    log_schema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", LongType()),
        StructField("lastName", StringType()),
        StructField("length", DoubleType()),
        StructField("level", DoubleType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()),
        StructField("sessionId", LongType()),
        StructField("song", StringType()),
        StructField("status", LongType()),
        StructField("ts", LongType()),
        StructField("userAgent", StringType()),
        StructField("userId", StringType())
    ])

    # read log data file
    df = spark.read.json(log_data, schema=log_schema)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    columns = ["CAST(userId AS INT) as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(*columns).dropDuplicates(["user_id"])

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(f"{output_data}/users_table/")

    # create timestamp column from original timestamp column
    get_timestamp = None # Not sure about this one
    df = df.withColumn('timestamp', F.expr("CAST(ts AS TIMESTAMP)"))

    # create datetime column from original timestamp column
    get_datetime = None # Not sure about this one

    # extract columns to create time table
    columns = [
        "timestamp AS start_time", 
        "hour(timestamp) AS hour",
        "day(timestamp) AS day",
        "month(timestamp) AS month",
        "weekofyear(timestamp) AS week",
        "year(timestamp) AS year",
        "weekday(timestamp) AS weekday",
    ]

    time_table = df.selectExpr(*columns).dropDuplicates(["start_time"])  

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_data}/time_table/")

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(f"{output_data}/songs_table/")
    artists_df = spark.read.parquet(f"{output_data}/artists_table/")

    j_songs_artists = songs_df.join(artists_df, on='artist_id').drop(*["location", "latitude", "longitude"])
    j_logs = df.join(j_songs_artists, [df.song==j_songs_artists.title, df.artist==j_songs_artists.name])
    j_final = j_logs.join(time_table, j_logs.timestamp==time_table.start_time)

    # extract columns from joined song and log datasets to create songplays table 
    columns = ["monotonically_increasing_id() AS songplay_id ", "start_time", "userId AS user_id", "level", "song_id", "artist_id", "sessionId AS session_id", "location", "userAgent AS user_agent"]
    songplays_table = j_final.selectExpr(*columns)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.withColumn('year', F.expr("year(start_time)"))
    songplays_table = songplays_table.withColumn('month', F.expr("month(start_time)"))
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(f"{output_data}/songplays_table/")

    
def main():
    """The main entrypoint of our application."""
    spark = create_spark_session()
    
    input_data = None
    output_data = None

    if app_cfg.APP_DEV:
        input_data = app_cfg.DATA_DIR
        output_data = app_cfg.ARTIFACTS_DIR

        # Check if we need to create the output directory.
        if not os.path.exists(output_data):
            try:
                os.mkdir(output_data)
            except Exception as err:
                print("[!] Failed to create output directory: {app_cfg.ARTIFACTS_DIR}. Exiting.")
    else:
        input_data = "s3a://udacity-dend/"
        output_data = "s3a://udacity-dend"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    print(app_cfg.get_logo())

    if app_cfg.APP_DEV:
        print("""
        DEV MODE

            - Verbosity is increased.
            - Data are read and written locally.


        Set `APP_DEV = False` in `config.py` to run on AWS.
    """)
    
    main()
