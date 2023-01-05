from Spark_2.code._utils_code import *

#creating spark session
spark = createSparkSession()

#Reading the log file of ghtorrent and renaming column name
ghtorrentdf = createDfTorrent(spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details","../../resource/ghtorrent-logs.txt")

#creating df with required fields by splitting
torrent_df_extract = splitRequiredColumns(ghtorrentdf)
print("Printing the torrent df")
torrent_df_extract.show()

# total number of lines
count = count_number_of_lines(torrent_df_extract)
print("Total number of lines")
count.show()

# total number of warnings
log_level_count = filter_by_loglevel(torrent_df_extract, "LogLevel", "WARN")
print("Count of warning level log")
log_level_count.show()

# How many repositories where processed in total? Use the api_client lines only.
countApiClient = repoProcessedApiClient(torrent_df_extract,"api_client.rb", "repository_torrent")
print("Count of repos processed by api_client only")
countApiClient.show()

# Which client did most HTTP requests
max_http_client_req = maxClientReq(torrent_df_extract, "request_url", "ghtorrent_client_id")
print("Clients with http request only")
max_http_client_req.show(1)

# Which client did most FAILED HTTP requests
failed_count = getFailedCount(torrent_df_extract, "Request_status_ext", "%Failed%", "ghtorrent_client_id", "max_failed_req_client")
print("Count of failed http client only")
failed_count.show(1)

#What is the most active hour of day?
df_active_hours = active_hours(torrent_df_extract)
print("Count of active hours")
df_active_hours.show(1)

# What is the most active repository
count_repo_active = countRepo(torrent_df_extract, "repository_torrent", "active_repo_used")
print("Most used active repository")
count_repo_active.show(1)