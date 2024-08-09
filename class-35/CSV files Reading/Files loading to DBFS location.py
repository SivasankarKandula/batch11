# Databricks notebook source
# Define the sample data as a single string
sample_data = "id,name,age,score\n1,siva,aa,85\n2,Alice,24,92\n3,Bob,30,78.9\n4,Eve,22,95.2"

# Replace 'dbfs:/FileStore/Schema_example_file.csv' with the desired DBFS file path
dbfs_file_path = 'dbfs:/FileStore/Schema_example_file.csv'

# Write the sample data to the specified DBFS file path
dbutils.fs.put(dbfs_file_path, sample_data, overwrite=True)




# COMMAND ----------

# Define the sample data as a single string
sample_data = """name,age,score,city
John,28,85.5,New York
Alice,24,aaa,Los Angeles
Bob,acb,78.9,Chicago
Eve,fgh,95.2,125"""

# Replace 'dbfs:/FileStore/InferSchema_example_file.csv' with the desired DBFS file path
dbfs_file_path = 'dbfs:/FileStore/InferSchema_example_file.csv'

# Write the sample data to the specified DBFS file path
dbutils.fs.put(dbfs_file_path, sample_data, overwrite=True)



# COMMAND ----------

# Define the sample data as a single string
sample_data = """name,birth_date
John,20-05-1992
Alice,1995/02/02
Bob,1992-25-05
Eve,1998-04-30"""

# Replace 'dbfs:/FileStore/DateFormat_example_file.csv' with the desired DBFS file path
dbfs_file_path = 'dbfs:/FileStore/DateFormat_example_file.csv'

# Write the sample data to the specified DBFS file path
dbutils.fs.put(dbfs_file_path, sample_data, overwrite=True)


# COMMAND ----------

csv_data = """name,age,country
John,25,USA
Alice,32,Canada
Robert,forty,England
Mary,28,Germany
Michael,35,France
Julia,thirty,Russia
David,40,Italy
Sarah,twenty-five,Australia"""

file_path = "/dbfs/fileStore/Mode_example_file.csv"

dbutils.fs.put(file_path, csv_data, overwrite=True)



# COMMAND ----------

# CSV data as a string
csv_data = """name,age,country
John,25,USA
Alice,32,Canada
Robert,forty,England
Mary,28,Germany
Michael,35,France
Julia,"thirty,
five",Russia
David,40,Italy
Sarah,twenty-five,"Aust
ralia"
"""

# File path
file_path = '/dbfs/FileStore/Multiline_example_file.csv'

# Write the CSV data to the file
dbutils.fs.put(file_path, csv_data, overwrite=True)


# COMMAND ----------

# CSV data as a string
csv_data = """name,age,country
John,25,USA
Alice,thirty,Canada
Robert,40,England
Mary,twenty-eight,Germany
Michael,35,France
Julia,thirty,Russia
David,forty,Italy
Sarah,25,Australia
"""

# File path
file_path = '/dbfs/FileStore/enforceSchema_example_file.csv'

# Write the CSV data to the file
dbutils.fs.put(file_path, csv_data, overwrite=True)


# COMMAND ----------

# Define the data as a multi-line string
data = """Name;Age;City;Occupation
John;30;New York;Engineer
Alice;25;San Francisco;Data Scientist
Michael;35;Los Angeles;Teacher
"""

# File path in DBFS
file_path = "/filestore/t1.csv"

# Write the data to the file using dbutils.fs.put()
dbutils.fs.put(file_path, data, overwrite=True)

print("Data loaded successfully to", file_path)


# COMMAND ----------

# CSV data for data_2023_07_01.csv
data_2023_07_01 = """name,age,country
John,25,USA
Alice,32,Canada
Robert,40,England
"""

# CSV data for data_2023_07_02.csv
data_2023_07_02 = """name,age,country
Mary,28,Germany
Michael,35,France
Julia,30,Russia
"""

# CSV data for data_2023_07_03.csv
data_2023_07_03 = """name,age,country
David,40,Italy
Sarah,25,Australia
"""

# File paths for each data file
file_path_2023_07_01 = '/dbfs/FileStore/data_2023_07_01.csv'
file_path_2023_07_02 = '/dbfs/FileStore/data_2023_07_02.csv'
file_path_2023_07_03 = '/dbfs/FileStore/data_2023_07_03.csv'

# Write CSV data to each file using dbutils.fs.put
dbutils.fs.put(file_path_2023_07_01, data_2023_07_01, overwrite=True)
dbutils.fs.put(file_path_2023_07_02, data_2023_07_02, overwrite=True)
dbutils.fs.put(file_path_2023_07_03, data_2023_07_03, overwrite=True)


# COMMAND ----------


