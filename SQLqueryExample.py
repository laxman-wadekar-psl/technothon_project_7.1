import csv
import pyarrow.parquet as pq
import mysql.connector
import json
from pyspark.sql import SparkSession
import psycopg2
import pandas as pd

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

metadata_json=open("C:/Users/laxman_wadekar/Desktop/TECHNOTHON/metadata.json","r")
jsondata=metadata_json.read()
obj=json.loads(jsondata)

# read file code

source_file_path=str(obj["source"]["file_path"])
sql_query=obj["source"]["sql_query_to_run"]
# sql_query=None

print(sql_query)
# print(type(sql_query))
temp_view=str(obj["source"]["temp_view"])


attributes=str(obj["source"]["attributes"][0])
print(attributes)


#
# def read_data_from_csv_file(file_path):
#     df = spark.read.csv(file_path, inferSchema=True, header=True)
#     if sql_query is None:
#         data=df.select("*")
#         print(data.count())
#         return data
#     else:
#         df.createOrReplaceTempView(temp_view)
#         new_df = spark.sql(sql_query)
#         # print(new_df.show())
#         print(new_df.count())
#         return new_df
#
# data = read_data_from_csv_file(source_file_path)
# print(data)
#



