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


source_database=str(obj["source"]["database"])
source_table_name=str(obj["source"]["table_name"])
source_user=str(obj["source"]["user"])
source_password=str(obj["source"]["password"])
source_host=str(obj["source"]["host"])
source_port=str(obj["source"]["port"])

def read_data_from_pgsql(table_name):
    # connection = psycopg2.connect(user=source_user,
    #                               password=source_password,
    #                               host=source_host,
    #                               port=source_port,
    #                               database=source_database)
    # cursor = connection.cursor()
    # cursor.execute(f"SELECT * FROM {table_name}")
    # rows = cursor.fetchall()
    # column_names = [desc[0] for desc in cursor.description]
    # cursor.close()
    # connection.close()
    # return spark.createDataFrame(rows, column_names)
    url = f"jdbc:postgresql://{source_host}/{source_database}"
    properties = {"user": f"{source_user}", "password": f"{source_password}","driver": "org.postgresql.Driver"}
    # read data from PostgreSQL into a PySpark DataFrame
    df = spark.read.jdbc(url=url, table=table_name, properties=properties)
    # display the contents of the DataFrame
    # data=df.select("*").show(df.count(),False)

    return df




data =read_data_from_pgsql(source_table_name)
print(data)


target_format_type = str(obj["target"]["format_type"])
target_file_format = str(obj["target"]["file_format"])
target_file_path = str(obj["target"]["file_path"])

def write_data_to_csv_file(target_file_path):
    # data.write.csv(target_file_path)
    data.write.csv(target_file_path, header=True,mode="overwrite")

write_data_to_csv_file(target_file_path)