from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("Spark").getOrCreate()

data =[("James ","","Smith","36636","M",3000),
              ("Michael ","Rose","","40288","M",4000),
              ("Robert ","","Williams","42114","M",4000),
              ("Maria ","Anne","Jones","39192","F",4000),
              ("Jen","Mary","Brown","","F",-1)]
columns=["firstname","middlename","lastname","dob","gender","salary"]
df=spark.createDataFrame(data,columns)
df.write.parquet("C:/Users/laxman_wadekar/Desktop/people")

spark.stop()