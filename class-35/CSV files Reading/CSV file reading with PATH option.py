# Databricks notebook source
# MAGIC %md
# MAGIC We can read the file from DBFS location to dataframe by giving the csv file path.
# MAGIC
# MAGIC Go to data --> DBFS --> copy the file path 
# MAGIC
# MAGIC Define the file path: Replace 'path/to/your_file.csv' with the actual path to your CSV file.
# MAGIC
# MAGIC Read the CSV file: We use spark.read.csv() to read the CSV file. The header=True option tells Spark to treat the first row as the header containing column names.

# COMMAND ----------

# Method:1  Reading the file by using Path
# Your actual file path
file_path = 'dbfs:/FileStore/Path_example_file.csv'

# Read the CSV file into a DataFrame
df = spark.read.csv(file_path, header=True)

# Show the contents of the DataFrame
df.show()

# COMMAND ----------

##  We can also read the file by giving direct path

# Read the CSV file into a DataFrame
df = spark.read.csv('dbfs:/FileStore/Path_example_file.csv', header=True)

# Show the contents of the DataFrame
df.show()


# COMMAND ----------

### Method:2 Reading the file with option


file_path = 'dbfs:/FileStore/Path_example_file.csv'

# Read the CSV file into a DataFrame with options
df = spark.read.option("header", "true") \
                         .csv(file_path)

df.show()

# COMMAND ----------

### Method:3 Reading the file by using format

file_path = 'dbfs:/FileStore/Path_example_file.csv'
# Read the CSV file into a DataFrame with format and options
df = spark.read.format("csv") \
                          .load(file_path)

# Show the contents of the DataFrame
df.show()

# COMMAND ----------


