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

# print(str(obj["version"]))
# print(str(obj["metadata"]))
# print(str(obj["source"]["file_path"]))


# ------------------------------------------------------------------------------
# read file code
source_format_type = str(obj["source"]["format_type"])
source_file_format = str(obj["source"]["file_format"])
source_file_path=str(obj["source"]["file_path"])

def read_data_from_csv_file(file_path):
    df = spark.read.csv(file_path, inferSchema=True, header=True)
    # data= df.select("*").show(df.count(),False)
    print(df.count())
    return df


def read_data_from_parquet_file(file_path):
    df = spark.read.parquet(file_path)
    # data = df.select("*").show(df.count(),False)
    print(df.count())
    return df


source_database=str(obj["source"]["database"])
source_table_name=str(obj["source"]["table_name"])
source_user=str(obj["source"]["user"])
source_password=str(obj["source"]["password"])
source_host=str(obj["source"]["host"])
source_port=str(obj["source"]["port"])


# def read_data_from_pgsql(db_config):
#         url = f"postgres://{source_user}:{source_password}@{source_host}:5432/{source_database}"
#         properties = {
#          "user": db_config['source_user'],
#          "password": db_config['source_password'],
#          "driver": "org.postgresql.Driver"
#         }
#
#         df = spark.read.jdbc(url=url, table=source_table_name, properties=properties)
#         return df

def read_data_from_pgsql(source_table_name):
    connection = psycopg2.connect(user=source_user, password=source_password, host=source_host, port=source_port, database=source_database)
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    cursor.close()
    connection.close()
    return spark.createDataFrame(rows, column_names)

if source_format_type == "file":
    if source_file_format == "csv":
        read_data = read_data_from_csv_file(source_file_path)
    elif source_file_format == "parquet":
        read_data = read_data_from_parquet_file(source_file_path)
    else:
        raise ValueError("Unsupported file format: {}".source_file_format(source_file_format))
    print(read_data)
elif source_format_type == "DB":
    db_type = "pgsql"
    # db_config = {
    #     source_database, source_table_name, source_user, source_password, source_host, source_port
    # }
    if db_type == "pgsql":
        read_data = read_data_from_pgsql(source_table_name)
    else:
        raise ValueError("Unsupported DB type: {}".source_format_type(db_type))
    print(read_data)
else:
    raise ValueError("Unsupported source: {}".source_format_type(source_format_type))



# -----------------------------------------------------------------------------------
# write file code

target_format_type = str(obj["target"]["format_type"])
target_file_format = str(obj["target"]["file_format"])
target_file_path = str(obj["target"]["file_path"])



# write function for  files
def write_data_to_csv_file(target_file_path):
    # data.write.csv(target_file_path)
    read_data.write.mode("overwrite").csv(target_file_path, header=True)


def write_data_to_parquet_file(target_file_path):
    # data.write.parquet(target_file_path)
    read_data.write.parquet(target_file_path)






def write_data_to_pgsql(data, db_config):
    url = "jdbc:postgresql://{host}:{port}/{database}".format(
    host=db_config['host'], port=db_config['port'], database=db_config['database'])
    properties = {
        "user": db_config['user'],
        "password": db_config['password'],
        "driver": "org.postgresql.Driver"
        }
    data.write.jdbc(url=url, table=db_config['table_name'], mode="overwrite", properties=properties)






if target_format_type == "file":

    if target_file_format == "csv":
        write_data_to_csv_file(target_file_path)
        # read_data.write.csv(target_file_path)
    elif target_file_format == "parquet":
        write_data_to_parquet_file(target_file_path)
        # read_data.write.parquet(target_file_path)
    else:
        raise ValueError("Unsupported file format: {}".target_format_type(target_file_format))
    print("Data is written...")
elif target_format_type == "DB":
    db_type = "pgsql"
    db_config = { database, table_name, user, password, host, port}
    if db_type == "pgsql":
        write_data_to_pgsql(db_config)
    else:
        raise ValueError("Unsupported DB type: {}".target_format_type(target_file_format))
    print("Data is written")
else:
    raise ValueError("Unsupported source: {}".target_format_type(target_file_format))

spark.stop()