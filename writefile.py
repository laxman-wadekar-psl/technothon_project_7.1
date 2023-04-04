import json
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

metadata_json=open("C:/Users/laxman_wadekar/Desktop/TECHNOTHON/metadata.json","r")
jsondata=metadata_json.read()
obj=json.loads(jsondata)

source_file_path=str(obj["source"]["file_path"])

def read_data_from_csv_file(source_file_path):
    df = spark.read.parquet(source_file_path)
    # data = df.select("*").show(df.count(),False)
    # print(df.count())
    return df
    # print("data reading done")

df1 =read_data_from_csv_file(source_file_path)

# print(data)









# write file conditions
target_format_type = str(obj["target"]["format_type"])
target_file_path=str(obj["target"]["file_path"])
target_file_type = str(obj["target"]["file_format"])

# write function for  files
def write_data_to_csv_file(target_file_path):
    # data.write.parquet(target_file_path)
    df1.write.csv(target_file_path)


def write_data_to_parquet_file(target_file_path):
    # data.write.parquet(target_file_path)
    df1.write.parquet(target_file_path)

if target_format_type == "file":

    if target_file_type == "csv":
        write_data_to_csv_file(target_file_path)
    elif target_file_type == "parquet":
        write_data_to_parquet_file(target_file_path)
    else:
        raise ValueError("Unsupported file format: {}".target_format_type(file_format))
    print("Data is written...")
elif target_format_type == "DB":
    db_type = "mysql"
    db_config = {
        'user': 'myuser',
        'password': 'mypassword',
        'host': 'localhost',
        'database': 'mydatabase'
    }
    if db_type == "mysql":
        write_data_to_mysql(db_config)
    else:
        raise ValueError("Unsupported DB type: {}".target_format_type(db_type))
    print("Data is written")
else:
    raise ValueError("Unsupported source: {}".target_format_type(source))

