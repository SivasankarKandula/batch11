# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, BooleanType, DateType, TimestampType, ArrayType, MapType
from datetime import datetime


# Define the schema for the DataFrame
schema = StructType([
    StructField('string_col', StringType(), True),
    StructField('int_col', IntegerType(), True),
    StructField('float_col', FloatType(), True),
    StructField('double_col', DoubleType(), True),
    StructField('bool_col', BooleanType(), True),
    StructField('date_col', DateType(), True),
    StructField('timestamp_col', TimestampType(), True),
    StructField('array_col', ArrayType(StringType()), True),
    StructField('map_col', MapType(StringType(), IntegerType()), True)
])

# Define some sample data
data = [('string1', 1, 1.1, 2.2, True, datetime.strptime('2022-05-01',  '%Y-%m-%d'),  datetime.strptime('2022-05-01 09:00:00', '%Y-%m-%d %H:%M:%S'), ['a', 'b'], {'key1': 1, 'key2': 2}),
        ('string2', 2, 3.3, 4.4, False, datetime.strptime('2022-05-01', '%Y-%m-%d'), datetime.strptime('2022-06-15 12:34:56', '%Y-%m-%d %H:%M:%S'), ['c', 'd'], {'key3': 3, 'key4': 4})]

# Create a PySpark DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.display()


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType, MapType
from pyspark.sql.functions import col, to_timestamp, concat_ws, to_json
from datetime import datetime


# Define the PySpark schema for the DataFrame
schema = StructType([
    StructField('string_col', StringType(), True),
    StructField('int_col', IntegerType(), True),
    StructField('double_col', DoubleType(), True),
    StructField('timestamp_col', TimestampType(), True),
    StructField('array_col', ArrayType(IntegerType()), True),
    StructField('map_col', MapType(StringType(), IntegerType()), True)
])

# Create a PySpark DataFrame
df = spark.createDataFrame([
    ('1', 1, 1.0, datetime.strptime('2022-05-01 09:00:00', '%Y-%m-%d %H:%M:%S'), [1, 2, 3], {'a': 1, 'b': 2}),
    ('2', 2, 2.0, datetime.strptime('2022-05-01 09:00:00', '%Y-%m-%d %H:%M:%S'), [4, 5, 6], {'c': 3, 'd': 4})
], schema)

# Show the original DataFrame
df.show()

# Cast StringType to IntegerType
df = df.withColumn('int_col', col('string_col').cast('int'))

# Cast IntegerType to StringType
df = df.withColumn('string_col', col('int_col').cast('string'))

# Cast DoubleType to FloatType
df = df.withColumn('float_col', col('double_col').cast('float'))

# Cast TimestampType to StringType
df = df.withColumn('string_col', col('timestamp_col').cast('string'))

# Cast StringType to TimestampType
df = df.withColumn('timestamp_col', to_timestamp(col('string_col'), 'yyyy-MM-dd HH:mm:ss'))

# Cast ArrayType to StringType
df = df.withColumn('string_col', concat_ws(',', col('array_col')))

# Cast MapType to StringType
df = df.withColumn('string_col', to_json(col('map_col')))

# Show the final DataFrame
df.show()


# COMMAND ----------


