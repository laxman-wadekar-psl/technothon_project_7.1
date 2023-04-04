import json

metadata_json = open("metadata.json", "r")
json_data = metadata_json.read()
obj = json.loads(json_data)

source_format_type = str(obj["source"]["format_type"])
source_file_format = str(obj["source"]["file_format"])
source_file_path = str(obj["source"]["file_path"])
source_database = str(obj["source"]["database"])
source_table_name = str(obj["source"]["table_name"])
source_user = str(obj["source"]["user"])
source_password = str(obj["source"]["password"])
source_host = str(obj["source"]["host"])
source_port = str(obj["source"]["port"])
source_db_type = str(obj["source"]["db_type"])


temp_view = str(obj["source"]["temp_view"])
sql_query = obj["source"]["sql_query_to_run"]
source_attributes = (obj["source"]["attributes"])

columns = []
sql_query_attributes = None
if source_attributes is not None:
    length = len(obj["source"]["attributes"])
    for i in range(length):
        columns.append(str(obj["source"]["attributes"][i]["col_name"]))
    result_string = ','.join(columns)
    sql_query_attributes = f"select {result_string} from {temp_view}"




target_format_type = str(obj["target"]["format_type"])
target_file_format = str(obj["target"]["file_format"])
target_file_path = str(obj["target"]["file_path"])
target_database = str(obj["target"]["database"])
target_table_name = str(obj["target"]["table_name"])
target_user = str(obj["target"]["user"])
target_password = str(obj["target"]["password"])
target_host = str(obj["target"]["host"])
target_port = str(obj["target"]["port"])
target_db_type = str(obj["target"]["db_type"])
