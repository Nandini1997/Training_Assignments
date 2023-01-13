from Spark_1.code._utils_code import *

#creating spark session
spark = sparkSession()

#creating transaction dataframe
trans_df = trasactiondf(spark,"../../resource/transaction.csv")
#creating user dataframe
user_df = userdf(spark,"../../resource/user.csv")

#joining two df
trans_user_join=joining_trans_user(trans_df, user_df)

#Count of unique locations where each product is sold
print("Count of unique locations where each product is sold")
countLocation = countDistinctLocation(trans_user_join, "location ")
countLocation.show()

#Find out products bought by each user.
print("Find out products bought by each user")
user_product = prod_by_users(trans_user_join, "userid", "product_description")
user_product.show()

#Total spending done by each user on each product
print("Total spending done by each user on each product")
spendings = sum_spending_by_prod(trans_user_join, "userid", "product_id", "price")
spendings.show()