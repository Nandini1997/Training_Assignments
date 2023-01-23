from spark_sql_assignment.Assignment_3.code.utility import *
from Pyspark.modularized_spark_session.spark_session import *

#creating spark session
spark = sparkSessionCreation()

#creating dataframe
emp_df = createDataframe(spark)
emp_df.show()

#select firstname, lastname and salary from Dataframe
print("select firstname, lastname and salary from Dataframe")
nameDf = extractFromNested(emp_df, "name")
nameDf.show()

#Add Country, department, and age column in the dataframe.
print("Add Country, department, and age column in the dataframe")
emp_df_updated = addColumnsEmpDf(emp_df)
emp_df_updated.show()

#change the value of salary column
print("change the value of salary column")
emp_salary_update = updateSalaryColumn(emp_df_updated)
emp_salary_update.show()

#change the datatype of DOB and salary to string
print("change the datatype of DOB and salary to string")
emp_updated_df = updateDatatype(emp_salary_update)
emp_updated_df.show()

#update schema(renaming nested columns)
print("update schema(renaming nested columns)")
schemaUpdatedDf = updateNestedSchema(emp_updated_df)
schemaUpdatedDf.printSchema()

#name of employee with salary maximum
print("name of employee with salary maximum")
maxEmployeedf = maxOfSalary(emp_updated_df)
maxEmployeedf.show(1)

#dropping department and age column
print("dropping department and age column")
updateDropColumn = droppingColumn(schemaUpdatedDf)
updateDropColumn.show()

#finding distinct values of salary
print("finding distinct values of salary")
distinctSalaryDob = findDistinctValue(updateDropColumn)
distinctSalaryDob.show()