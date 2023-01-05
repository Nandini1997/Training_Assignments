import unittest
from Spark_2.code._utils_code import *


class UtilityTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark = createSparkSession()
        cls.spark = spark

    def testCreateDfTorrent(self):
        torrent_df = createDfTorrent(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details","../../test_resource/ghtorrent_test_log.txt")
        count = torrent_df.count()
        self.assertGreaterEqual(count, 0)

    def testColumnName(self):
        torrent_df = createDfTorrent(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "../../test_resource/ghtorrent_test_log.txt")
        actual_output = torrent_df.columns
        expected_output = ['LogLevel', 'timestamp', 'ghtorrent_details']
        self.assertEqual(actual_output, expected_output)

    def testSplitRequiredColumn(self):
        torrent_df = createDfTorrent(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "../../test_resource/ghtorrent_test_log.txt")
        torrent_df_extract = splitRequiredColumns(torrent_df)
        actual_output = torrent_df_extract.columns
        expected_output = ['LogLevel', 'timestamp', 'ghtorrent_client_id', 'downloader_id', 'repository_torrent', 'Request_status_ext', 'Request_status', 'request_url']
        self.assertEqual(actual_output,expected_output)

    def testCountNumberLines(self):
        torrent_df = createDfTorrent(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "../../test_resource/ghtorrent_test_log.txt")
        no_of_lines = torrent_df.count()
        self.assertEqual(no_of_lines, 54)

    def testLogLevelCount(self):
        torrent_df = createDfTorrent(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "../../test_resource/ghtorrent_test_log.txt")
        expected_count = filter_by_loglevel(torrent_df, "LogLevel", "WARN").first()["warn_count"]
        self.assertEqual(expected_count, 3)

    def testcountApiClient(self):
        torrent_df = createDfTorrent(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "../../test_resource/ghtorrent_test_log.txt")
        torrent_df_extract = splitRequiredColumns(torrent_df)
        expected_count = repoProcessedApiClient(torrent_df_extract, "api_client.rb", "repository_torrent").first()["api_client_repo_count"]
        self.assertEqual(expected_count,12)

    def testMaxClientReq(self):
        torrent_df = createDfTorrent(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "../../test_resource/ghtorrent_test_log.txt")
        torrent_df_extract = splitRequiredColumns(torrent_df)
        expected_ouput = maxClientReq(torrent_df_extract, "request_url", "ghtorrent_client_id").first()["ghtorrent_client_id"]
        actual_output = " ghtorrent-5 "
        self.assertEqual(expected_ouput,actual_output)

    def testGetFailedCount(self):
        torrent_df = createDfTorrent(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "../../test_resource/ghtorrent_test_log.txt")
        torrent_df_extract = splitRequiredColumns(torrent_df)
        expected_output = getFailedCount(torrent_df_extract, "Request_status_ext", "%Failed%", "ghtorrent_client_id", "max_failed_req_client").first()["max_failed_req_client"]
        actual_output = " ghtorrent-13 "
        self.assertEqual(expected_output,actual_output)

    def testActiveHourse(self):
        torrent_df = createDfTorrent(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "../../test_resource/ghtorrent_test_log.txt")
        torrent_df_extract = splitRequiredColumns(torrent_df)
        expected_output = active_hours(torrent_df_extract).first()["active_hours"]
        actual_output = 16
        self.assertEqual(expected_output,actual_output)

    def testCountMaxRepo(self):
        torrent_df = createDfTorrent(self.spark, "_c0", "LogLevel", "_c1", "timestamp", "_c2", "ghtorrent_details",
                                     "../../test_resource/ghtorrent_test_log.txt")
        torrent_df_extract = splitRequiredColumns(torrent_df)
        expected_output = countRepo(torrent_df_extract, "repository_torrent", "active_repo_used").first()["active_repo_used"]
        actual_output = " ghtorrent.rb"
        self.assertEqual(expected_output,actual_output)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()