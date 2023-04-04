# sample code to read csv file and print its data and write it into another csv file
# import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
# C:/Users/laxman_wadekar/Desktop/Dataset_Technothon/archive/acc_17.csv
# D:/PySpark/test.csv
df = spark.read.csv("D:/PySpark/test.csv",inferSchema=True, header= True)
# df.show()
# df.printSchema()
df.select(df.columns[0:3]).show(10)
# spark.sql.debug.maxToStringFields
# df.select("CASENUM","REGION").display(10)

# df.write.csv("C:/Users/laxman_wadekar/Desktop/output11",header=True)


spark.stop()
