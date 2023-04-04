import os

filepaths = ["D:/PySpark/test.csv", "folder1/folder/soundfile.parquet"]

for fp in filepaths:
    # Split the extension from the path and normalise it to lowercase.
    ext = os.path.splitext(fp)[-1].lower()

    # Now we can simply use == to check for equality, no need for wildcards.
    if ext == ".csv":
        print(fp, "is an csv")

    elif ext == ".parquet":
        print(fp, "is a parquet file!")
    else:
        print(fp, "is an unknown file format.")