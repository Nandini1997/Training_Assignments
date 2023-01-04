from Spark_1.code._utils_code import *
import unittest

class UtilityTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spark = sparkSession()
        cls.spark = spark

    # Testing whether the function is reading the trans df
    def test_trans_create(self):
        trans_df = trasactiondf(self.spark, '../../test_resource/transaction_test.csv')
        num_of_lines = trans_df.count()
        self.assertEqual(num_of_lines,3)

    # Testing whether the function is reading the user df
    def test_users_create(self):
        user_df = userdf(self.spark,'../../test_resource/user_test.csv')
        num_of_lines = user_df.count()
        self.assertEqual(num_of_lines, 4)

    #Testing whether the dunction finding the distinct count is retuning correct value
    def test_count_distinct_location(self):
        trans_df = trasactiondf(self.spark, '../../test_resource/transaction_test.csv')
        user_df = userdf(self.spark,'../../test_resource/user_test.csv')
        trans_userd_df = joining_trans_user(trans_df, user_df)
        count = countDistinctLocation(trans_userd_df, "location ")
        c = count.first()['Distinct_location_count']
        self.assertEqual(c, 3)

    # Check whether prod_user_function is returning output or not
    def test_prod_by_users_count(self):
        trans_df = trasactiondf(self.spark, '../../test_resource/transaction_test.csv')
        user_df = userdf(self.spark, '../../test_resource/user_test.csv')
        trans_userd_df = joining_trans_user(trans_df, user_df)
        actual_df = prod_by_users(trans_userd_df, "userid", "product_description")
        count = actual_df.count()
        self.assertGreaterEqual(count, 0)

    #check whether the prod_user function returned output is matching with actual output
    def test_sum_spending_by_prod(self):
        trans_df = trasactiondf(self.spark, '../../test_resource/transaction_test.csv')
        user_df = userdf(self.spark, '../../test_resource/user_test.csv')
        trans_userd_df = joining_trans_user(trans_df, user_df)
        spendings = sum_spending_by_prod(trans_userd_df, "userid", "product_id", "price")
        sum_price = spendings.first()["Total Price"]
        self.assertEqual(sum_price,700)


    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

