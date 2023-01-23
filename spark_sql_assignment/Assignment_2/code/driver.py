from Pyspark.modularized_spark_session.spark_session import *
from spark_sql_assignment.Assignment_2.code.utility import *

#create spark session
spark = sparkSessionCreation()

#create dataframe
df = createDaraFrame(spark)

#total amount exported to each country of each product
pivotdf = pivotDf(df)
pivotdf.show()

#unpivot the pivot df
unpivitdf = unpivot(pivotdf)
unpivitdf.show()