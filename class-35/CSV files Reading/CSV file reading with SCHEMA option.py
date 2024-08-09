# Databricks notebook source
# Reading the file directly without using option SCHEMA

df=spark.read.csv('dbfs:/FileStore/Schema_example_file.csv',header= True )

df.display()

# In this example score is float type but in result decimals are not coming and age is integer but its showing character also. now see what will happen while using Struct type.

# COMMAND ----------

# Reading the file directly with using option SCHEMA(Struct type)



from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType




file_path = 'dbfs:/FileStore/Schema_example_file.csv'

# Define the custom schema
custom_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("score", FloatType(), True)
])

# Read the CSV file into a DataFrame with the custom schema
df = spark.read.csv(file_path, header=True, schema=custom_schema)

# Show the contents of the DataFrame
df.show()







# COMMAND ----------

df.printSchema()

# COMMAND ----------


