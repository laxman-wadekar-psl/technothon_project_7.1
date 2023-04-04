import psycopg2
import pandas as pd

conn = psycopg2.connect(
    database = "sample_db",
    user ="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
cursor_obj = conn.cursor()
cursor_obj.execute("select * from persons")
#result = cursor_obj.fetchall()
result = pd.DataFrame(cursor_obj.fetchall())
print(result)
result.to_csv('Person.csv')
# table_data = []
# for row in result:
# table_data.append(list(row))
# print(table_data)