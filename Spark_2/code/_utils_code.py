from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, trim
from pyspark.sql.functions import split, to_timestamp, hour
from pyspark.sql.types import TimestampType
from Pyspark.modularized_spark_session.spark_session import sparkSessionCreation


def createSparkSession():
    spark = sparkSessionCreation()
    return spark

def createDfTorrent(spark,col1,rename_col_1,col2,rename_col_2,col3,rename_col_3):
    df = spark.read.csv("../../resource/ghtorrent-logs.txt")
    torrent_df = df.withColumnRenamed(col1,rename_col_1).withColumnRenamed(col2,rename_col_2).withColumnRenamed(col3,rename_col_3)
    return torrent_df

def splitRequiredColumns(torrent_log_df):
    torrent_cliendId_extract = torrent_log_df \
        .withColumn("ghtorrent_client_id", split(col("ghtorrent_details"), "--").getItem(0)) \
        .withColumn("repository", split(col("ghtorrent_details"), "--").getItem(1)) \
        .withColumn("downloader_id", split(col("ghtorrent_client_id"), "-").getItem(1)) \
        .withColumn("repository_torrent", split(col("repository"), ':').getItem(0)) \
        .withColumn("Request_status_ext", split(col("repository"), ':').getItem(1)) \
        .withColumn("Request_status", split(col("Request_status_ext"), ",").getItem(0)) \
        .drop(col("repository")) \
        .withColumn("request_url", split(col("ghtorrent_details"), "URL:").getItem(1)) \
        .drop(col("ghtorrent_details"))
    return torrent_cliendId_extract

def count_number_of_lines(df):
    total_number_of_lines = df.select(count("*").alias("total_count"))
    return total_number_of_lines

def filter_by_loglevel(df,column_name, filter_string):
    warning_count = df.filter(col(column_name) == filter_string).select(count("*").alias("warn_count"))
    return warning_count

def repoProcessedApiClient(df,filter_string, col_name):
    api_client_repo_count = df.filter(trim(col(col_name)) == filter_string).select(
        count("*").alias("api_client_repo_count"))
    return api_client_repo_count

def maxClientReq(df, column_name, group_by_column):
    total_http_client_req = df.filter(col(column_name).isNotNull()).groupBy(group_by_column).count()
    max_http_client_req_count = total_http_client_req.sort(col("count").desc())
    return max_http_client_req_count

def getFailedCount(df, column_name, string, groupby_rename_col, renamed_col):
    failed_count = df.filter(col(column_name).like(string)).groupBy(
        groupby_rename_col).count()
    max_failed_client_req = failed_count.withColumnRenamed(groupby_rename_col, renamed_col)
    max_failed_client_red = max_failed_client_req.sort(col("count").desc())
    return max_failed_client_red

def active_hours(df):
    df_with_hours = df.withColumn("timestamp_new", to_timestamp(col("timestamp"))) \
        .withColumn("hours", hour(col("timestamp_new"))).groupBy("hours").count()
    df_active_hours = df_with_hours.withColumnRenamed("hours", "active_hours")
    df_res = df_active_hours.sort(col("count").desc())
    return df_with_hours

def countRepo(df, groupby_col, renamed_col):
    count_repo = df.groupBy(groupby_col).count()
    count_repo_active = count_repo.withColumnRenamed(groupby_col, renamed_col)
    active_repository = count_repo_active.sort(col("count").desc())
    return active_repository
