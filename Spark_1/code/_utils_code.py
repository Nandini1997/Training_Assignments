from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import split,col,countDistinct, sum

def sparkSession():
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    return spark

def trasactiondf(spark,path):
    transactionSchema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("userid", IntegerType(), True),
        StructField("price", IntegerType(), True),
        StructField("product_description", StringType(), True)
    ])

    transaction_df = spark.read.option("header", True).schema(transactionSchema).csv(path)
    return transaction_df

def userdf(spark, path):
    userSchema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("emailid", StringType(), True),
        StructField("nativelanguage", StringType(), True),
        StructField("location ", StringType(), True)
    ])
    user_df = spark.read.option("header", True).schema(userSchema).csv(path)
    return user_df

def joining_trans_user(transaction_df, user_df):
    transaction_user_df = transaction_df.join(user_df, transaction_df.userid == user_df.user_id, "inner")
    return transaction_user_df

def countDistinctLocation(transaction_user_df,location):
    count_of_location = transaction_user_df.select(countDistinct(location).alias("Distinct_location_count"))
    return count_of_location

def prod_by_users(trans_user_df,userid,prod_desc):
    products_users = trans_user_df.select(col(userid), col(prod_desc))
    return products_users

def sum_spending_by_prod(transaction_user_df,userid, prodid, price):
    spending_each_price = transaction_user_df.groupBy(userid, prodid).agg(sum(price).alias("Total Price"))
    return spending_each_price
