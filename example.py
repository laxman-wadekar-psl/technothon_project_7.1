import json
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

metadata_json=open("C:/Users/laxman_wadekar/Desktop/TECHNOTHON/metadata.json","r")
jsondata=metadata_json.read()
obj=json.loads(jsondata)


# print(str(obj["version"]))
# print(str(obj["metadata"]))

# print(str(obj["source"]["file_path"]))
file_path=str(obj["source"]["file_path"])
# source_format_type = str(obj["source"]["format_type"])
# print(source_format_type)
# source_file_type = str(obj["source"]["file_format"])
# print(source_file_type)

# cols=str(obj["source"]["attributes"]["col_name"])
# print(cols)

def read_data_from_csv_file(file_path):
    df = spark.read.parquet(file_path)
    data = df.select("*").show(df.count(),False)
    # print(df.count())
    return data

data =read_data_from_csv_file(file_path)

print(data)