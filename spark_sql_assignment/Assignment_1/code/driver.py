from Pyspark.modularized_spark_session.spark_session import *
from spark_sql_assignment.Assignment_1.code.utility import *

#calling spark session
spark = sparkSessionCreation()

#creating dataframe
emp_df = createDataframe(spark)

#finding the highest salary
df = employeeWithHighestSal(emp_df)
df.show(1)

#Select first row from each department group.
firstRowDf = firstRowPerDepart(emp_df)
firstRowDf.show()

#Highest, lowest, average, and total salary for each department group.
aggrDf = agreggateSalary(emp_df)
aggrDf.show()