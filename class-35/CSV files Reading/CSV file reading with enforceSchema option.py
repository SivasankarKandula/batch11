# Databricks notebook source
# MAGIC %md
# MAGIC To enforce a specific schema while reading CSV data in Spark, you can use the enforceSchema option. This option ensures that the data in the DataFrame adheres to the specified schema. If the data types in the CSV file match the schema, Spark will use it. If there are any type mismatches, Spark will try to cast the data to match the specified schema.

# COMMAND ----------

# Reading the file directly without using option INFERSCHEMA

df=spark.read.csv('/dbfs/FileStore/enforceSchema_example_file.csv',header= True )

df.display()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder.appName("CSV Read Example").getOrCreate()

# CSV file path
csv_file_path = "/dbfs/FileStore/enforceSchema_example_file.csv"

# Define the schema for the CSV data
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True)
])

try:
    # Read the CSV file with enforceSchema option
    df = spark.read.option("header", "true") \
                   .option("enforceSchema", "true") \
                   .schema(schema) \
                   .csv(csv_file_path)

    # Show the DataFrame
    df.show()

except Exception as e:
    print(f"Exception encountered: {e}")


# COMMAND ----------


