# Databricks notebook source
# MAGIC %md
# MAGIC The inferSchema option is a parameter used while reading a CSV file in PySpark to automatically infer the data types for each column in the DataFrame. When inferSchema is set to True, Spark will scan a sample of the data in the CSV file to determine the data types of each column based on the actual data it encounters.

# COMMAND ----------

# Reading the file directly without using option INFERSCHEMA

df=spark.read.csv('dbfs:/FileStore/InferSchema_example_file.csv',header= True )

df.display()

# 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Create a SparkSession
spark = SparkSession.builder.appName("CSVReadExample").getOrCreate()

# Replace 'path/to/sample_data.csv' with the actual filename and path
file_path = 'dbfs:/FileStore/InferSchema_example_file.csv'

# Define the schema for the DataFrame
custom_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("score", FloatType(), True),
    StructField("city", StringType(), True)
])

# Read the CSV file into a DataFrame with the defined schema (without inferSchema)
df_without_infer = spark.read.csv(file_path, header=True, schema=custom_schema)

# Show the contents of the DataFrame
df_without_infer.show()


# COMMAND ----------


file_path = 'dbfs:/FileStore/InferSchema_example_file.csv'

# Read the CSV file into a DataFrame with inferred schema
df_infer_true = spark.read.csv(file_path, header=True, inferSchema=True)

# Show the contents of the DataFrame
df_infer_true.show()


# COMMAND ----------


