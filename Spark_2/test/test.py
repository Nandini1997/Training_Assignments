import unittest
from pyspark.sql.types import StructType,StructField,StringType, IntegerType, LongType
from Spark_2.code._utils_code import *
from Pyspark.test.test_modularized_utils import *
from Pyspark.modularized_spark_session.spark_session import sparkSessionCreation

class UtilityTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark = sparkSessionCreation()
        cls.spark = spark

    # input log file sample data
    def log_input_data(self):
        input_schema = StructType([
            StructField("LogLevel", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("ghtorrent_details", StringType(), True)
        ])
        data = [("DEBUG", "2017-03-23T11:15:14+00:00", "ghtorrent-30 -- retriever.rb: Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists"),
                ("DEBUG", "2017-03-23T11:15:14+00:00","ghtorrent-30 -- retriever.rb: Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists"),
("INFO", "2017-03-23T11:08:13+00:00", "ghtorrent-8 -- api_client.rb: Successful request. URL: https://api.github.com/repos/Particular/NServiceBus.Persistence.ServiceFabric/pulls/10/comments?per_page=100, Remaining: 3333, Total: 110 ms"),
("DEBUG", "2017-03-23T12:02:06+00:00", "ghtorrent-31 -- ghtorrent.rb: Association of commit 7ce873512d609ed53a6c38eb32d5a1a706712c0d with repo ovh/cds exists"),
("INFO", "2017-03-23T09:11:17+00:00", "ghtorrent-5 -- api_client.rb: Successful request. URL: https://api.github.com/repos/javier-serrano/web-ui-a2?per_page=100, Remaining: 3381, Total: 153 ms"),
("WARN", "2017-03-23T10:33:43+00:00", "ghtorrent-23 -- ghtorrent.rb: Not a valid email address: greenkeeper[bot]")]
        input_df = self.spark.createDataFrame(data, input_schema)
        return input_df

    # Testing df with required fields by splitting
    def testSplitRequiredColumn(self):
        transformed_df = splitRequiredColumns(self.log_input_data())
        expected_schema = StructType([
            StructField("LogLevel", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("ghtorrent_client_id", StringType(), True),
            StructField("downloader_id", StringType(), True),
            StructField("repository_torrent", StringType(), True),
            StructField("Request_status_ext", StringType(), True),
            StructField("Request_status", StringType(), True),
            StructField("request_url", StringType(), True)
        ])
        data = [
            ("DEBUG", "2017-03-23T11:15:14+00:00", "ghtorrent-30", "30", "retriever.rb",
             "Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists",
             "Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists", ""),
            ("INFO", "2017-03-23T11:08:13+00:00", "ghtorrent-8", "8", "api_client.rb", "Successful request. URL",
             "Successful request. URL",
             "https://api.github.com/repos/Particular/NServiceBus.Persistence.ServiceFabric/pulls/10/comments?per_page=100, Remaining: 3333, Total: 110 ms"),
            ("DEBUG", "2017-03-23T12:02:06+00:00", "ghtorrent-31", "31", "ghtorrent.rb",
             "Association of commit 7ce873512d609ed53a6c38eb32d5a1a706712c0d with repo ovh/cds exists",
             "Association of commit 7ce873512d609ed53a6c38eb32d5a1a706712c0d with repo ovh/cds exists", ""),
            ("INFO", "2017-03-23T09:11:17+00:00", "ghtorrent-5", "5", "api_client.rb", "Successful request. URL",
             "Successful request. URL",
             "https://api.github.com/repos/javier-serrano/web-ui-a2?per_page=100, Remaining: 3381, Total: 153 ms"),
            ("WARN", "2017-03-23T10:33:43+00:00", "ghtorrent-23", "23", "ghtorrent.rb", "Not a valid email address",
             "Not a valid email address", "")
        ]
        expected_df = self.spark.createDataFrame(data,expected_schema)
        self.assertTrue(test_schema(transformed_df, expected_df))

    # Testing total number of lines
    def testCountNumberLines(self):
        transformed_df = count_number_of_lines(self.log_input_data())
        expected_schema = StructType([
            StructField("total_count", LongType(), False)
        ])
        data = [(6,)]
        expected_df = self.spark.createDataFrame(data, expected_schema)
        self.assertTrue(test_schema(transformed_df,expected_df))
        self.assertTrue(test_data(transformed_df,expected_df))

    # Testing total number of warnings loglevel
    def testLogLevelCount(self):
        transformed_df = filter_by_loglevel(self.log_input_data(), "LogLevel", "WARN")
        expected_schema = StructType([
            StructField("warn_count", LongType(), False)
        ])
        data = [(1,)]
        expected_df = self.spark.createDataFrame(data,expected_schema)
        self.assertTrue(test_schema(transformed_df,expected_df))
        self.assertTrue(test_data(transformed_df,expected_df))

    # Testing which api_client
    def testcountApiClient(self):
        transformed_df = repoProcessedApiClient(splitRequiredColumns(self.log_input_data()),"api_client.rb", "repository_torrent")
        expected_schema = StructType([
            StructField("api_client_repo_count", LongType(), False)
        ])
        data = [(2,)]
        expected_df = self.spark.createDataFrame(data, expected_schema)
        self.assertTrue(test_schema(transformed_df,expected_df))
        self.assertTrue(test_data(transformed_df,expected_df))

    # Testing client with HTTP requests count
    def testMaxClientReq(self):
        transformed_df = maxClientReq(splitRequiredColumns(self.log_input_data()), "request_url", "ghtorrent_client_id")
        expected_schema = StructType([
            StructField("ghtorrent_client_id", StringType(), True),
            StructField("count", LongType(), False)
        ])
        exped_data = [("ghtorrent-8 ",1),("ghtorrent-5 ",1)]
        expected_df = self.spark.createDataFrame(exped_data,expected_schema)
        self.assertTrue(test_schema(transformed_df,expected_df))
        self.assertTrue(test_data(transformed_df,expected_df))

    # Testing which client did most FAILED HTTP requests
    def testGetFailedCount(self):
        transformed_df = getFailedCount(splitRequiredColumns(self.log_input_data()), "Request_status_ext", "%Failed%", "ghtorrent_client_id", "max_failed_req_client")
        expected_schema = StructType([
            StructField("max_failed_req_client", StringType(), True),
            StructField("count", LongType(), False)
        ])
        expected_df = self.spark.createDataFrame([], schema=expected_schema)
        self.assertTrue(test_schema(transformed_df,expected_df))
        self.assertTrue(test_data(transformed_df,expected_df))

    # Testing the active hour of day
    def testActiveHourse(self):
        transformed_data = active_hours(self.log_input_data())
        expected_schema = StructType([
            StructField("hours", IntegerType(), True),
            StructField("count", LongType(), False)
        ])
        data = [(16,4),(17,1),(14,1)]
        expected_df = self.spark.createDataFrame(data, expected_schema)
        self.assertTrue(test_schema(transformed_data, expected_df))
        self.assertTrue(test_data(transformed_data,expected_df))

    # Testing what is the most active repository
    def testCountMaxRepo(self):
        transformed_df = countRepo(splitRequiredColumns(self.log_input_data()), "repository_torrent", "active_repo_used")
        expected_schema = StructType([
            StructField("active_repo_used", StringType(), True),
            StructField("count", LongType(), False)
        ])
        data = [(" api_client.rb",2),(" ghtorrent.rb",2),(" retriever.rb",2)]
        expected_df = self.spark.createDataFrame(data, expected_schema)
        self.assertTrue(test_schema(transformed_df, expected_df))
        self.assertTrue(test_data(transformed_df, expected_df))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()