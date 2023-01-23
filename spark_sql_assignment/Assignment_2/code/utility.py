from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def createDaraFrame(spark):
    data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]
    schema = StructType([
    StructField("Product", StringType(), True),
    StructField("Amount", IntegerType(), True),
    StructField("Country", StringType(), True)
    ])

    df=spark.createDataFrame(data=data, schema=schema)
    return df

def pivotDf(df):
    pivot_df = df.groupBy("Product").pivot("Country").sum("Amount")
    return pivot_df

def unpivot(df):
    unviot_df = df.selectExpr("Product", "stack(2,'China',China,'USA', USA) as (Country, Amount)").where('Amount is not null')
    return unviot_df