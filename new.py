import csv
import pyarrow.parquet as pq
import mysql.connector

def read_data_from_csv_file(file_path):
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        data = list(reader)
    return data


def read_data_from_parquet_file(file_path):
    table = pq.read_table(file_path)
    data = table.to_pandas()
    return data.values.tolist()


def read_data_from_mysql(db_config):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM my_table")
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data




source = "file"
file_path = "D:/PySpark/test.csv"

# file_path = "C:/Users/laxman_wadekar/Desktop/Dataset_Technothon/yellow_tripdata_2022-11.parquet"

#Create source class here

if source == "file":
    file_format = file_path.split('.')[-1]
    if file_format == "csv":
        data = read_data_from_csv_file(file_path)
    elif file_format == "parquet":
        data = read_data_from_parquet_file(file_path)
    else:
        raise ValueError("Unsupported file format: {}".format(file_format))
    print(data)
elif source == "DB":
    db_type = "mysql"
    db_config = {
        'user': 'myuser',
        'password': 'mypassword',
        'host': 'localhost',
        'database': 'mydatabase'
    }
    if db_type == "mysql":
        data = read_data_from_mysql(db_config)
    else:
        raise ValueError("Unsupported DB type: {}".format(db_type))
    print(data)
else:
    raise ValueError("Unsupported source: {}".format(source))
