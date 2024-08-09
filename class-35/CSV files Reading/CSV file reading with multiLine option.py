# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC When reading a CSV file in Spark, you can use the multiLine option to handle multiline records. By default, Spark assumes that each row of the CSV file corresponds to a single line. However, sometimes, a single record can span multiple lines in the CSV file, which can cause issues when reading data.
# MAGIC
# MAGIC To handle multiline records in the CSV file, you can set the multiLine option to true. This option tells Spark to consider newlines within a quoted field as part of the field and not as the end of a record.

# COMMAND ----------

# Reading the file directly without using option INFERSCHEMA

df=spark.read.csv('/dbfs/FileStore/Multiline_example_file.csv',header= True )

df.display()


# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("CSV Read Example").getOrCreate()

# CSV file path
csv_file_path = "/dbfs/FileStore/Multiline_example_file.csv"

# Read the CSV file with multiLine option
df = spark.read.option("header", "true") \
               .option("multiLine", "true") \
               .csv(csv_file_path)

# Show the DataFrame
df.display()


# COMMAND ----------


