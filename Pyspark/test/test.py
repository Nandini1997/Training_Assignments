from Pyspark.Code._utils_code import *
from Pyspark.test.test_modularized_utils import *
from Pyspark.modularized_spark_session.spark_session import sparkSessionCreation
import datetime
import unittest
class UnitTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark=sparkSessionCreation()
        cls.spark=spark

    def prod_input_data(self):
        inputProdSchema = StructType([
            StructField("ProductName", StringType(), True),
            StructField("IssueDate", LongType(), True),
            StructField("Price", IntegerType(), True),
            StructField("Brand", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Product number", IntegerType(), True)
        ])
        input_data = [("Washing Machine", 1648770933000, 20000, "Samsung", "India", 1000),
                      ("Refrigerator", 1648770999000, 35000, "LG", "", 2000)]
        input_df = self.spark.createDataFrame(input_data, schema=inputProdSchema)
        return input_df

    def source_input_data(self):
        source_schema = StructType([
            StructField("SourceId", IntegerType(), True),
            StructField("TransactionNumber", IntegerType(), True),
            StructField("Language", StringType(), True),
            StructField("ModelNumber", IntegerType(), True),
            StructField("StartTime", StringType(), True),
            StructField("ProductNumber", IntegerType(), True)
        ])
        input_data = [(150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", 1000)]
        input_df = self.spark.createDataFrame(input_data, schema=source_schema)
        return input_df
    def test_transformation(self):
        tranformed_df = to_timestamp_con(self.prod_input_data(), "Issue_Date_timestamp", "IssueDate", "Issue_Date_Form")
        expectedSchema = StructType([
            StructField("ProductName", StringType(), True),
            StructField("IssueDate", LongType(), True),
            StructField("Price", IntegerType(), True),
            StructField("Brand", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Product number", IntegerType(), True),
            StructField("Issue_Date_Form", DateType(), True)
        ])

        output_data = [("Washing Machine", 1648770933000, 20000, "Samsung", "India", 1000, datetime.date(2022, 4, 1)),
                     ("Refrigerator", 1648770999000, 35000, "LG", "", 2000, datetime.date(2022, 4, 1))]
        expected_df = self.spark.createDataFrame(output_data, schema=expectedSchema)
        self.assertTrue(test_schema(tranformed_df, expected_df))
        self.assertTrue(test_data(tranformed_df, expected_df))

    def test_snakecase(self):
        transformed = to_snake_case(self.source_input_data())
        expected_schema = StructType([
            StructField("source_id", IntegerType(),True),
            StructField("transaction_number", IntegerType(), True),
            StructField("language", StringType(), True),
            StructField("model_number", IntegerType(), True),
            StructField("start_time", StringType(), True),
            StructField("product_number", IntegerType(), True)
            ])
        expected_data = [(150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", 1000)]
        expected_df = self.spark.createDataFrame(expected_data, schema=expected_schema)
        self.assertTrue(transformed,expected_df)


    def test_to_unix_timestamp(self):
        transformed_df = to_unix_timestamp(self.source_input_data(), "epoch_seconds", "StartTime")
        expected_schema = StructType([
            StructField("SourceId", IntegerType(), True),
            StructField("TransactionNumber", IntegerType(), True),
            StructField("Language", StringType(), True),
            StructField("ModelNumber", IntegerType(), True),
            StructField("StartTime", StringType(), True),
            StructField("ProductNumber", IntegerType(), True),
            StructField("epoch_seconds", LongType(), True)
        ])
        data = [(150711,123456,"EN",456789,"2021-12-27T08:20:29.842+0000",1000,1640593229)]
        expected_df = self.spark.createDataFrame(data, expected_schema)
        self.assertTrue(test_schema(transformed_df, expected_df))
        self.assertTrue(test_data(transformed_df, expected_df))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()