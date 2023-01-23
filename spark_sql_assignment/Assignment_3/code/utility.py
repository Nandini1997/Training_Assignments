from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import to_date, datediff, current_date, lit, col, floor

def createDataframe(spark):
    data = [
    (("James","","Smith"),"03011998",'M',3000),
    (("Michael","Rose",""),"10111998",'M',20000),
    (("Robert","","Williams"),"02012000",'M',3000),
    (("Maria","Anne","Jones"),"03011998",'F',11000),
    (("Jen","Mary","Brown"),"04101998",'F',10000),
    ]
    nameSchema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True)
    ])
    rootSchema = StructType([
    StructField("name", StructType(nameSchema), True),
    StructField("dob", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
    ])
    df = spark.createDataFrame(data=data, schema=rootSchema)
    return df

def extractFromNested(df, col_name):
    df_res = df.select(col_name+".*")
    return df_res

def addColumnsEmpDf(df):
    df_ret = df.withColumn("dob", to_date(col("dob"),"ddMMyyyy"))\
    .withColumn("Age", floor(datediff(current_date(),col("dob"))/365.25))\
    .withColumn("departments", lit("Data Engineer"))\
    .withColumn("Country", lit("India"))
    return df_ret

def updateSalaryColumn(df):
    df_ret = df.withColumn("salary", col("salary")*0.1+col("salary"))
    return df_ret

def updateDatatype(df):
    df_ret = df.withColumn("dob", col("dob").cast(StringType())).withColumn("salary", col("salary").cast(StringType()))
    return df_ret

def updateNestedSchema(df):
    nameSchemaUpdated = StructType([
    StructField("firstposition", StringType(), True),
    StructField("secondposition", StringType(), True),
    StructField("thirdposition", StringType(), True)
    ])
    df_ret = df.select(col("name").cast(nameSchemaUpdated), col('dob'), col("gender"), col("salary"), col("Age"), col("departments"), col("Country"))
    return df_ret

def maxOfSalary(df):
    df_ret=df.sort(col("salary").desc()).select(col('name'))
    return df_ret

def droppingColumn(df):
    df_ret = df.drop("departments","Age")
    return df_ret

def findDistinctValue(df):
    df_ret = df.select(col("salary"), col("dob")).distinct()
    return df_ret