from os import path
import csv
import os
def main():
	input_path="D:/PySpark/test.csv"

	source_isFile=str(path.isfile(input_path))

	print(source_isFile)

	for fp in input_path:
		ext=os.path.splitext(fp)[-1].lower()
		if ext== ".csv":
			csv_file=1
		elif ext== ".parquet":
			parquet_file=1

	if(source_isFile):
		print("source is file")
		# if(csv_file==1):
		# 	print("CSV FILE")
		# elif(parquet_file==1):
		# 	print("PARQUET FILE")


if __name__== "__main__":
	main()