# Databricks notebook source
imports

# COMMAND ----------

variable

# COMMAND ----------

dbutils.fs.ls("dbfs:/batch09")

# COMMAND ----------


# Directory path with CSV files
csv_directory_path = "dbfs:/FileStore"

# Use pathGlobFilter to read only CSV files starting with 'data_' and ending with '.csv'
df = spark.read.option("header", "true").option("pathGlobFilter", "*siva.csv") \
               .csv(csv_directory_path )

# Show the DataFrame
df.display()


# COMMAND ----------

df.createTempView('emp')

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore")

# COMMAND ----------

# Directory path with CSV files
csv_directory_path = "dbfs:/FileStore"
# Path glob filter pattern: Read CSV files starting with "Loan"
path_glob_filter_pattern = "Loan*.csv"

# Read CSV files with the specified path glob filter
df = spark.read.option("header", "true") \
               .option("pathGlobFilter", path_glob_filter_pattern) \
               .csv(csv_directory_path)

# Show the DataFrame
df.display()



# COMMAND ----------


