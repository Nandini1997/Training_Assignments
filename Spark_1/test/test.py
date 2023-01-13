from Spark_1.code._utils_code import *
import unittest
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, LongType
from Pyspark.modularized_spark_session.spark_session import sparkSessionCreation
from Pyspark.test.test_modularized_utils import *
class UtilityTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark = sparkSessionCreation()
        cls.spark = spark

    #transaction sample input data
    def transaction_input(self):
        input_schema =  StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("userid", IntegerType(), True),
        StructField("price", IntegerType(), True),
        StructField("product_description", StringType(), True)
        ])
        data = [(3300101,1000001,101,700,"mouse"), (3300102,1000002,102,900,"keyboard"),(3300103,1000003,103,34000,"tv")]
        input_df = self.spark.createDataFrame(data, input_schema)
        return input_df

    #user input sample data
    def user_input(self):
        userSchema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("emailid", StringType(), True),
            StructField("nativelanguage", StringType(), True),
            StructField("location ", StringType(), True)
        ])
        data = [(101,"abc.123@gmail.com","hindi","mumbai"),(102,"jhon@gmail.com","english","usa"),(103,"madan.44@gmail.com","marathi","nagpur"),(104,"local.88@outlook.com","tamil","chennai")]
        input_df = self.spark.createDataFrame(data, userSchema)
        return input_df

    #Testing whether the dunction finding the distinct count is retuning correct value
    def test_count_distinct_location(self):
        trans_userd_df = joining_trans_user(self.transaction_input(), self.user_input())
        transaction_df = countDistinctLocation(trans_userd_df, "location ")
        expected_schema = StructType([
            StructField("Distinct_location_count", LongType(), False)
        ])
        data = [(3,)]
        expected_df = self.spark.createDataFrame(data, expected_schema)
        self.assertTrue(test_schema(transaction_df, expected_df))
        self.assertTrue(test_data(transaction_df, expected_df))

    # Check whether prod_user_function is returning output or not
    def test_prod_by_users_count(self):
        trans_userd_df = joining_trans_user(self.transaction_input(), self.user_input())
        trasaction_df = prod_by_users(trans_userd_df, "userid", "product_description")
        expected_schema = StructType([
            StructField("userid", IntegerType(), True),
            StructField("product_description", StringType(), True)
        ])
        data = [(101, "mouse"),(103, "tv"),(102, "keyboard")]
        expected_df = self.spark.createDataFrame(data, expected_schema)
        self.assertTrue(test_schema(trasaction_df,expected_df))
        self.assertTrue(test_data(trasaction_df, expected_df))


    #check whether the prod_user function returned output is matching with actual output
    def test_sum_spending_by_prod(self):
        trans_userd_df = joining_trans_user(self.transaction_input(), self.user_input())
        transaction = sum_spending_by_prod(trans_userd_df, "userid", "product_id", "price")
        expected_schema = StructType([
            StructField("userid", IntegerType(), True),
            StructField("product_id",IntegerType(), True),
            StructField("Total Price",LongType(), True)
        ])
        data = [(101,1000001,700),(103,1000003,34000),(102,1000002,900)]
        expected_df = self.spark.createDataFrame(data, expected_schema)
        self.assertTrue(test_schema(transaction, expected_df))
        self.assertTrue(test_data(transaction,  expected_df))



    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

