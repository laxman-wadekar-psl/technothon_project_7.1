import csv
import pyarrow.parquet as pq
import mysql.connector
import json
from pyspark.sql import SparkSession
import schedule
import time


spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

metadata_json=open("C:/Users/laxman_wadekar/Desktop/TECHNOTHON/metadata.json","r")
jsondata=metadata_json.read()
obj=json.loads(jsondata)

# ------------------------------------------------------------------------------
# read file code
# ------------------------------------------------------------------------------

source_format_type = str(obj["source"]["format_type"])
source_file_format = str(obj["source"]["file_format"])
source_file_path=str(obj["source"]["file_path"])


source_database=str(obj["source"]["database"])
source_table_name=str(obj["source"]["table_name"])
source_user=str(obj["source"]["user"])
source_password=str(obj["source"]["password"])
source_host=str(obj["source"]["host"])
source_port=str(obj["source"]["port"])
source_db_type=str(obj["source"]["db_type"])

temp_view=str(obj["source"]["temp_view"])
sql_query=obj["source"]["sql_query_to_run"]

source_attributes = (obj["source"]["attributes"])

columns = []
sql_query_attributes=None
if source_attributes is not None:
    length = len(obj["source"]["attributes"])
    for i in range(length):
        columns.append(str(obj["source"]["attributes"][i]["col_name"]))
    result_string=','.join(columns)
    sql_query_attributes=f"select {result_string} from {temp_view}"



def read_data_from_csv_file(file_path):
    try:
        df = spark.read.csv(file_path, inferSchema=True, header=True)
        if sql_query is None:
            if sql_query_attributes is None:
                data = df.select("*")
                # df.show()
                # print(data.count())
                return data
            else:
                df.createOrReplaceTempView(temp_view)
                new_df = spark.sql(sql_query_attributes)
                # new_df.show()
                # print(new_df.count())
                return new_df
        else:
            df.createOrReplaceTempView(temp_view)
            new_df = spark.sql(sql_query)
            # new_df.show()
            # print(new_df.count())
            return new_df
        print("Data Read Completed from csv")
    except:
        print("Data reading failed from csv")


def read_data_from_parquet_file(file_path):
    try:
        df = spark.read.parquet(file_path)
        if sql_query is None:
            if sql_query_attributes is None:
                data = df.select("*")
                # df.show()
                # print(data.count())
                return data
            else:
                df.createOrReplaceTempView(temp_view)
                new_df = spark.sql(sql_query_attributes)
                # new_df.show()
                # print(new_df.count())
                return new_df
        else:
            df.createOrReplaceTempView(temp_view)
            new_df = spark.sql(sql_query)
            # print(new_df.show())
            # print(new_df.count())
            return new_df
        print("Data Read Completed from parquet")
    except:
        print("Data reading failed from parquet")



def read_data_from_pgsql(table_name):
    try:
        url = f"jdbc:postgresql://{source_host}/{source_database}"
        properties = {"user": f"{source_user}", "password": f"{source_password}", "driver": "org.postgresql.Driver"}
        # read data from PostgreSQL into a PySpark DataFrame
        df = spark.read.jdbc(url=url, table=table_name, properties=properties)
        # display the contents of the DataFrame
        # data=df.select("*").show(df.count(),False)
        if sql_query is None:
            if sql_query_attributes is None:
                data = df.select("*")
                # df.show()
                # print(data.count())
                return data
            else:
                df.createOrReplaceTempView(temp_view)
                new_df = spark.sql(sql_query_attributes)
                # new_df.show()
                # print(new_df.count())
                return new_df
        else:
            df.createOrReplaceTempView(temp_view)
            new_df = spark.sql(sql_query)
            # print(new_df.show())
            # print(new_df.count())
            return new_df
        print("Data Read Completed from PGSQL")
    except:
        print("Database connection failed can't read from PGSQL")


try:
    if source_format_type == "file":
        if source_file_format == "csv":
            read_data = read_data_from_csv_file(source_file_path)
        elif source_file_format == "parquet":
            read_data = read_data_from_parquet_file(source_file_path)
        else:
            raise ValueError("Unsupported file format: {}".source_file_format(source_file_format))
    elif source_format_type == "db":
        if source_db_type == "pgsql":
            read_data = read_data_from_pgsql(source_table_name)
        else:
            raise ValueError("Unsupported DB type: {}".source_format_type(source_db_type))
    else:
        raise ValueError("Unsupported source: {}".source_format_type(source_format_type))
except:
    print("An exception occurred...unsupported file type or file format")





# -----------------------------------------------------------------------------------
# write file code
# -----------------------------------------------------------------------------------


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



def write_data_to_csv_file(target_file_path):
    try:
        read_data.coalesce(1).write.csv(target_file_path, header=True, mode="overwrite")
        print("Data is written to CSV...")
    except:
        print("Data write to CSV is failed")

def write_data_to_parquet_file(target_file_path):
    try:
        read_data.coalesce(1).write.parquet(target_file_path,mode="overwrite")
        print("Data is written to Parquet...")
    except:
        print("Data write to Parquet is failed")

def write_data_to_pgsql(table_name):
    try:
        url = f"jdbc:postgresql://{target_host}/{target_database}"
        properties = {"user": f"{target_user}", "password": f"{target_password}", "driver": "org.postgresql.Driver"}
        read_data.coalesce(1).write.jdbc(url=url, table=table_name, mode="overwrite", properties=properties)
        print("Data is written to PGSQL...")
    except:
        print("Data write to pgsql is failed")


try:
    if target_format_type == "file":
        if target_file_format == "csv":
            write_data_to_csv_file(target_file_path)
        elif target_file_format == "parquet":
            write_data_to_parquet_file(target_file_path)
        else:
            raise ValueError("Unsupported file format: {}".source_file_format(source_file_format))
    elif target_format_type == "db":
        if target_db_type == "pgsql":
            write_data_to_pgsql(target_table_name)
        else:
            raise ValueError("Unsupported DB type: {}".source_format_type(source_db_type))
    else:
        raise ValueError("Unsupported source: {}".source_format_type(source_format_type))
except:
    print("An exception occurred...unsupported file type or file format")


spark.stop()


