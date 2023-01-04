from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,TimestampType,LongType
from pyspark.sql.functions import col,from_unixtime,to_timestamp, to_date, unix_timestamp
import re
from functools import reduce

def createSparkSession():
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    return spark

def read_prod_df(spark,path):
    prod_schema = StructType([
        StructField("ProductName", StringType(), True),
        StructField("IssueDate",LongType(),True),
        StructField("Price", IntegerType(),True),
        StructField("Brand", StringType(),True),
        StructField("Country", StringType(),True),
        StructField("Product number", IntegerType(),True)
    ])
    df = spark.read \
        .option("header", True) \
        .schema(prod_schema)\
        .csv(path)
    return df

def to_timestamp_con(prod_df, issue_date_to, column, Issue_Date_Form):
    df_ret = prod_df.withColumn(issue_date_to, from_unixtime(col(column)/1000))\
    .withColumn(Issue_Date_Form, to_date(issue_date_to))
    return df_ret

def fill_null_na(df):
    df_res = df.na.fill("").select(col("ProductName"), col("Price"), col("Brand"), col("Country"), col("Product number").alias("ProductNumber"), col("Issue_Date_Form"))
    return df_res

def createSource(spark, path):
    source_schema=StructType([
        StructField("SourceId",IntegerType(),True),
        StructField("TransactionNumber", IntegerType(), True),
        StructField("Language", StringType(),True),
        StructField("ModelNumber", IntegerType(),True),
        StructField("StartTime", TimestampType(),True),
        StructField("ProductNumber", IntegerType(), True)
    ])
    source_df = spark.read\
        .option("header", True)\
        .schema(source_schema)\
        .csv(path)
    return source_df

def to_snake_case(sou_df):
    column_list = sou_df.columns
    df_ret = reduce(lambda new_df, i: new_df.withColumnRenamed(i, re.sub(r'(?<!^)(?=[A-Z])', '_', i).lower()),
                    column_list, sou_df)
    return df_ret

def to_unix_timestamp(df, col_name, date_column):
    df_res = df.withColumn(col_name, unix_timestamp(col(date_column), "yyyy-dd-MM HH:mm:ss"))
    return df_res

def to_join_prod_source(df1,df2, filter_string):
    df_res = df1.join(df2, df1.ProductNumber==df2.product_number,"inner").filter(col("language")== filter_string)
    return df_res
