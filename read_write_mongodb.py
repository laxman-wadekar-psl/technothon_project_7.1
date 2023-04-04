import csv
import pyarrow.parquet as pq
import mysql.connector
import json
from pyspark.sql import SparkSession
import psycopg2
import pandas as pd
# import pymongo
from pymongo import MongoClient


spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

metadata_json=open("C:/Users/laxman_wadekar/Desktop/TECHNOTHON/metadata.json","r")
jsondata=metadata_json.read()
obj=json.loads(jsondata)

# read file code
source_format_type = str(obj["source"]["format_type"])
source_file_format = str(obj["source"]["file_format"])
source_file_path=str(obj["source"]["file_path"])


source_database=str(obj["source"]["database"])
source_table_name=str(obj["source"]["table_name"])
source_user=str(obj["source"]["user"])
source_password=str(obj["source"]["password"])
source_host=str(obj["source"]["host"])
print(source_host)
source_port=str(obj["source"]["port"])
source_db_type=str(obj["source"]["db_type"])

temp_view=str(obj["source"]["temp_view"])
sql_query=obj["source"]["sql_query_to_run"]




source_collection=str(obj["source"]["collection"])


def read_data_from_mongodb():
    client=MongoClient(f"{source_host},{source_port}")
    db=client[f"{source_database}"]
    col=db[f"{source_collection}"]
    df = spark.createDataFrame(col.find())
    # df=spark.read.format('com.mongodb.spark.sql.DefaultSource').option("uri",f"mongodb://{source_host}/{source_database}.{source_collection}").load()
    # client = MongoClient(db_config['host'], db_config['port'])
    # db = client[db_config['database']]
    # collection = db[db_config['collection']]
    # df = spark.createDataFrame(collection.find())
    if sql_query is None:
        data = df.select("*")
        print(data.count())
        return data
    else:
        df.createOrReplaceTempView(temp_view)
        new_df = spark.sql(sql_query)
        # print(new_df.show())
        print(new_df.count())
        return new_df


read_data_from_mongodb()

# write

target_format_type = str(obj["target"]["format_type"])
target_file_format = str(obj["target"]["file_format"])
target_file_path = str(obj["target"]["file_path"])


target_database=str(obj["target"]["database"])
target_table_name=str(obj["target"]["table_name"])
target_user=str(obj["target"]["user"])
target_password=str(obj["target"]["password"])
target_host=str(obj["target"]["host"])
target_port=str(obj["target"]["port"])
target_db_type=str(obj["target"]["db_type"])
