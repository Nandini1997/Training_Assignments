from Pyspark.modularized_spark_session.spark_session import sparkSessionCreation
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, row_number,avg,sum,max,min
from pyspark.sql.window import Window

def createSparkSession():
    spark = sparkSessionCreation()
    return spark

def createDataframe(spark):
    data = [("James", "Sales", 3000),
            ("Michael", "Sales", 4600),
            ("Robert", "Sales", 4100),
            ("Maria", "Finance", 3000),
            ("Raman", "Finance", 3000),
            ("Scott", "Finance", 3300),
            ("Jen", "Finance", 3900),
            ("Jeff", "Marketing", 3000),
            ("Kumar", "Marketing", 2000)]
    schema = StructType([
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("Salary", IntegerType(), True)
    ])
    emp_df = spark.createDataFrame(data=data, schema=schema)
    return emp_df

def employeeWithHighestSal(df):
    res_df = df.sort(col("Salary").desc()).select(col("Salary").alias("Highest Salary"))
    return res_df

def firstRowPerDepart(df):
    windowspec = Window.partitionBy("department").orderBy("employee_name")
    df = df.withColumn("row_num", row_number().over(windowspec)).where(col("row_num")==1)
    return df

def agreggateSalary(df):
    df = df.groupBy("department").agg(min("Salary").alias("Lowest Salary"), max("Salary").alias("Highest Salary"), avg("Salary").alias("Average"), sum("Salary").alias("Totals"))
    return df