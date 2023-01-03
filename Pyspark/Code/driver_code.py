from Pyspark.Code._utils_code import *

#calling spark session
spark = createSparkSession()

#creating product details df
prod_details_df=read_prod_df(spark)


# converting milliseconds to time stamp format and conveting to date format only
df_with_timestamp = to_timestamp_con(prod_details_df, "Issue_Date_timestamp")


#Fill null with nothing
df_prod_details_res = fill_null_na(df_with_timestamp)
print("Printing final df after transformations")
df_prod_details_res.show()

#creating source df
source_df = createSource(spark)

# Converting the column name from camel case to snake case
df_source_col_name = to_snake_case(source_df)

#Converting start_time to unix time seconds and adding it as new column
df_source_res = to_unix_timestamp(df_source_col_name,"epoch_seconds", "start_time")
print("Final source df after transformation")
df_source_res.show()

#Combine both the file and return the country EN
df_final_output = to_join_prod_source(df_prod_details_res,df_source_res, "EN")
print("Final output after joining and filtering")
df_final_output.show()