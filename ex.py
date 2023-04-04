# from pyspark.sql import SparkSession
# import variables as var
#
# spark = SparkSession.builder.appName("Data Ingestion Tool").getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")


class Action:
    # ----------------------------------------------------------------------------------------------------
    #            Read Data From Source
    # ----------------------------------------------------------------------------------------------------
    def read_data_from_csv_file(self, file_path):
        print("Data reading failed from csv")

    def read_data_from_parquet_file(self, file_path):
        print("Data reading failed from parquet")

    def read_data_from_pgsql(self, table_name):
       print("Database connection failed can't read from PGSQL")

    # ----------------------------------------------------------------------------------------------------
    #            Write Data To Target
    # ----------------------------------------------------------------------------------------------------
    def write_data_to_csv_file(self, file_path):
        print("Data is written to CSV...")


    def write_data_to_parquet_file(self, file_path):
       print("Data is written to Parquet...")



    def write_data_to_pgsql(self, table_name):
        print("Data is written to PGSQL...")


obj=Action()
obj.read_data_from_csv_file(1)
obj.read_data_from_parquet_file(1)
obj.read_data_from_pgsql(1)

obj.write_data_to_csv_file(1)
obj.write_data_to_parquet_file(1)
obj.write_data_to_pgsql(1)