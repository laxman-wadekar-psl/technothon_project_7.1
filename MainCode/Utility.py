from pyspark.sql import SparkSession
import variables as var


spark = SparkSession.builder.appName("Data Ingestion Tool").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

class Action:
# ----------------------------------------------------------------------------------------------------
#            Read Data From Source
# ----------------------------------------------------------------------------------------------------
    def read_data_from_csv_file(self, file_path):
        try:
            df = spark.read.csv(file_path, inferSchema=True, header=True)
            if var.sql_query is None:
                if var.sql_query_attributes is None:
                    data = df.select("*")
                    # df.show()
                    # print(data.count())
                    print("Data Read Completed from csv")
                    return data
                else:
                    df.createOrReplaceTempView(var.temp_view)
                    new_df = spark.sql(var.sql_query_attributes)
                    # new_df.show()
                    # print(new_df.count())
                    print("Data Read Completed from csv")
                    return new_df
            else:
                df.createOrReplaceTempView(var.temp_view)
                new_df = spark.sql(var.sql_query)
                # new_df.show()
                # print(new_df.count())
                print("Data Read Completed from csv")
                return new_df

        except:
            print("Data reading failed from csv")



    def read_data_from_parquet_file(self, file_path):
        try:
            df = spark.read.parquet(file_path)
            if var.sql_query is None:
                if var.sql_query_attributes is None:
                    data = df.select("*")
                    # df.show()
                    print(data.count())
                    print("Data Read Completed from parquet")
                    return data
                else:
                    df.createOrReplaceTempView(var.temp_view)
                    new_df = spark.sql(var.sql_query_attributes)
                    # new_df.show()
                    # print(new_df.count())
                    print("Data Read Completed from parquet")
                    return new_df
            else:
                df.createOrReplaceTempView(var.temp_view)
                new_df = spark.sql(var.sql_query)
                # print(new_df.show())
                # print(new_df.count())
                print("Data Read Completed from parquet")
                return new_df

        except:
            print("Data reading failed from parquet")

    def read_data_from_pgsql(self, table_name):
        try:
            url = f"jdbc:postgresql://{var.source_host}/{var.source_database}"
            properties = {"user": f"{var.source_user}", "password": f"{var.source_password}","driver": "org.postgresql.Driver"}
            # read data from PostgreSQL into a PySpark DataFrame
            df = spark.read.jdbc(url=url, table=table_name, properties=properties)
            # display the contents of the DataFrame
            # data=df.select("*").show(df.count(),False)
            if var.sql_query is None:
                if var.sql_query_attributes is None:
                    data = df.select("*")
                    # df.show()
                    # print(data.count())
                    print("Data Read Completed from PGSQL")
                    return data
                else:
                    df.createOrReplaceTempView(var.temp_view)
                    new_df = spark.sql(var.sql_query_attributes)
                    # new_df.show()
                    # print(new_df.count())
                    print("Data Read Completed from PGSQL")
                    return new_df
            else:
                df.createOrReplaceTempView(var.temp_view)
                new_df = spark.sql(var.sql_query)
                # print(new_df.show())
                # print(new_df.count())
                print("Data Read Completed from PGSQL")
                return new_df

        except:
            print("Database connection failed can't read from PGSQL")

# ----------------------------------------------------------------------------------------------------
#            Write Data To Target
# ----------------------------------------------------------------------------------------------------
    def write_data_to_csv_file(self, file_path,read_data):
        try:
            read_data.coalesce(1).write.csv(file_path,header= True, mode="overwrite")
            print("Data is written to CSV...")
        except:
            print("Data write to CSV is failed")

    def write_data_to_parquet_file(self, file_path, read_data):
        try:
            read_data.coalesce(1).write.parquet(file_path, mode="overwrite")
            print("Data is written to Parquet...")
        except:
            print("Data write to Parquet is failed")

    def write_data_to_pgsql(self, table_name, read_data):
        try:
            url = f"jdbc:postgresql://{var.target_host}/{var.target_database}"
            properties = {"user": f"{var.target_user}", "password": f"{var.target_password}", "driver": "org.postgresql.Driver"}
            read_data.coalesce(1).write.jdbc(url=url, table=table_name, mode="overwrite", properties=properties)
            print("Data is written to PGSQL...")
        except:
            print("Data write to pgsql is failed")