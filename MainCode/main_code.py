# importing Utility.py for reading and writing function
import Utility

# importing variables.py for taking variables
import variables as var

# creating object of utility class Action
obj = Utility.Action()

# ---------------------------------------------------------------------------------------------------
#                             Calling Read Function
# ---------------------------------------------------------------------------------------------------
class Main:
    # read data function
    def read_fun(self):
        try:
            if var.source_format_type == "file":
                if var.source_file_format == "csv":
                    read_data = obj.read_data_from_csv_file(var.source_file_path)
                    return read_data
                elif var.source_file_format == "parquet":
                    read_data = obj.read_data_from_parquet_file(var.source_file_path)
                else:
                    raise ValueError("Unsupported source file format", var.source_file_format)
            elif var.source_format_type == "db":
                if var.source_db_type == "pgsql":
                    read_data = obj.read_data_from_pgsql(var.source_table_name)
                    return read_data
                else:
                    raise ValueError("Unsupported source DB type: ", var.source_db_type)
            else:
                raise ValueError("Unsupported source format: ", var.source_format_type)
        except:
            print("An exception occurred...while reading...unsupported file type or file format")

# ---------------------------------------------------------------------------------------------------
#                             Calling Write Function
# ---------------------------------------------------------------------------------------------------

    # write data function
    def write_fun(self,read_data):
        try:
            if var.target_format_type == "file":
                if var.target_file_format == "csv":
                    obj.write_data_to_csv_file(var.target_file_path,read_data)
                elif var.target_file_format == "parquet":
                    obj.write_data_to_parquet_file(var.target_file_path,read_data)
                else:
                    print("Unsupported target file format: ", var.target_file_format)
            elif var.target_format_type == "db":
                if var.target_db_type == "pgsql":
                    obj.write_data_to_pgsql(var.target_table_name,read_data)
                else:
                    print("Unsupported target DB type: ", var.target_format_type)
            else:
                print("Unsupported Target format: ", var.target_format_type)
        except:
            print("An exception occurred...while writing...unsupported file type or file format")


# creating object for Main class
Main_obj = Main()

# calling read and write functions
read_data=Main_obj.read_fun()
Main_obj.write_fun(read_data)
