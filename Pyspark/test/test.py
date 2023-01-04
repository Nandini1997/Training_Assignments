from Pyspark.Code._utils_code import *
import unittest
class UnitTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark=createSparkSession()
        cls.spark=spark

    def test_creation_df(self):
        prod_df = read_prod_df(self.spark, "../../test_resource/Product_test.csv")
        no_of_row = prod_df.count()
        self.assertEqual(no_of_row, 2)
    def test_source_creation_df(self):
        source_df = createSource(self.spark, "../../test_resource/Source_test.csv")
        no_of_rows = source_df.count()
        self.assertEqual(no_of_rows, 1)

    def test_snake_case(self):
        source_df = createSource(self.spark, "../../test_resource/Source_test.csv")
        df = to_snake_case(source_df)
        actual_list = df.columns
        expected_list = ['source_id', 'transaction_number', 'language', 'model_number', 'start_time', 'product_number']
        self.assertEqual(actual_list, expected_list)

    def test_to_unix_timestamp(self):
        source_df = createSource(self.spark, "../../test_resource/Source_test.csv")
        df = to_unix_timestamp(source_df, "epoch_seconds", "StartTime")
        expected = df.first()["epoch_seconds"]
        actual = 1640593229
        self.assertEqual(expected,actual)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()