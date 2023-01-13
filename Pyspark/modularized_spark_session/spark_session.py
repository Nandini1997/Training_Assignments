from pyspark.sql.session import SparkSession
def sparkSessionCreation():
    spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
    return spark