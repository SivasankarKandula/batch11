# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC The mode parameter is an option available while reading data from a file in PySpark. It specifies the parsing mode for handling malformed records during the file read process. The mode option allows you to decide how Spark should handle records that do not conform to the expected schema or have parsing errors. There are three different modes available:
# MAGIC
# MAGIC PERMISSIVE (default mode):
# MAGIC
# MAGIC In the PERMISSIVE mode, Spark tries to parse and read as much data as possible, even if there are some malformed records or parsing errors in the input data.
# MAGIC If Spark encounters malformed records, it tries to infer the schema for the DataFrame, and any malformed records that cannot be parsed are represented as null values in the DataFrame for appropriate columns.
# MAGIC The PERMISSIVE mode is useful when dealing with dirty or unstructured data, as it allows you to read as much valid data as possible, even if there are some errors in the input.
# MAGIC
# MAGIC DROPMALFORMED:
# MAGIC
# MAGIC In the DROPMALFORMED mode, Spark drops the records that cannot be parsed correctly or do not conform to the expected schema.
# MAGIC If Spark encounters any malformed records, it simply skips those records and includes only the valid records in the resulting DataFrame.
# MAGIC The DROPMALFORMED mode is useful when you want to ignore the malformed records and keep only the clean data, discarding any data that cannot be successfully parsed.
# MAGIC
# MAGIC FAILFAST:
# MAGIC
# MAGIC In the FAILFAST mode, Spark throws an exception and fails immediately if it encounters any malformed records or parsing errors in the input data.
# MAGIC The FAILFAST mode is useful when you want to ensure that the input data strictly adheres to the expected schema, and any issues in the data should halt the reading process.
# MAGIC It is the most strict mode and may lead to job termination if the input data contains any malformed records.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

20 records-----5 records
read---mode()
permassive-------emno---null====defult---20
dropmalformed-------15
failfast-----------

# COMMAND ----------

write----mode('error')---mode('append')----mode('overwrite')

# COMMAND ----------

from pyspark.sql import SparkSession



# CSV file path
csv_file_path = "/dbfs/fileStore/Mode_example_file.csv"

# Read the CSV file with PERMISSIVE mode and columnNameOfCorruptRecord option
df = (spark.read.option("header", "true").option("mode" , 'permassive')                         
               .csv(csv_file_path))
df.printSchema()

# Show the DataFrame
df.show()


# COMMAND ----------



# COMMAND ----------

s1 = StructType( [StructField('name' , StringType(), True ) , 
             StructField('age' , IntegerType(), False)  ,
             StructField('country' , StringType() , True)
                ])

# COMMAND ----------

from pyspark.sql import SparkSession



# CSV file path
csv_file_path = "/dbfs/fileStore/Mode_example_file.csv"

# Read the CSV file with PERMISSIVE mode and columnNameOfCorruptRecord option
df = (spark.read.option("header", "true").option("mode" , 'DROPMALFORMED').schema(s1)                       
               .csv(csv_file_path))
df.printSchema()

# Show the DataFrame
df.show()

# COMMAND ----------

from pyspark.sql import SparkSession



# CSV file path
csv_file_path = "/dbfs/fileStore/Mode_example_file.csv"

# Read the CSV file with PERMISSIVE mode and columnNameOfCorruptRecord option
df = (spark.read.option("header", "true").option("mode" , 'failfast').schema(s1)                        
               .csv(csv_file_path))
df.printSchema()

# Show the DataFrame
df.show()

# COMMAND ----------

from pyspark.sql import SparkSession



# CSV file path
csv_file_path = "/dbfs/fileStore/Mode_example_file.csv"

# Read the CSV file with PERMISSIVE mode and columnNameOfCorruptRecord option
df = (spark.read.option("header", "true").option("mode" , 'permassive').schema(s1)                        
               .csv(csv_file_path))
df.printSchema()

# Show the DataFrame
df.show()


# COMMAND ----------

s1 = StructType( [StructField('name' , StringType(), True ) , 
             StructField('age' , IntegerType(), False)  ,
             StructField('country' , StringType() , True),
             StructField('test' , StringType() , True)      ])

# COMMAND ----------

from pyspark.sql import SparkSession



# CSV file path
csv_file_path = "/dbfs/fileStore/Mode_example_file.csv"

# Read the CSV file with PERMISSIVE mode and columnNameOfCorruptRecord option
df = (spark.read.option("header", "true").option("mode" , 'permassive').schema(s1).option("columnNameOfCorruptRecord", "test")                        
               .csv(csv_file_path))
df.printSchema()

# Show the DataFrame
df.show()


# COMMAND ----------

from pyspark.sql import SparkSession



# CSV file path
csv_file_path = "/dbfs/fileStore/Mode_example_file.csv"

# Read the CSV file with PERMISSIVE mode and columnNameOfCorruptRecord option
df = (spark.read.option("header", "true").option("mode" , 'permassive').schema(s1)                        
               .csv(csv_file_path))
df.printSchema()

# Show the DataFrame
df.show()


# COMMAND ----------



# Create a SparkSession
spark = SparkSession.builder.appName("CSV Read Example").getOrCreate()

# CSV file path
csv_file_path = "/dbfs/fileStore/Mode_example_file.csv"

# Define the schema for the CSV data
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True)
])

# Read the CSV file with DROPMALFORMED mode and the specified schema
df = spark.read.option("header", "true") \
               .option("mode", "DROPMALFORMED") \
               .option("inferSchema", "false") \
               .schema(schema) \
               .csv(csv_file_path)

# Show the DataFrame
df.display()



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder.appName("CSV Read Example").getOrCreate()

# CSV file path
csv_file_path = "/dbfs/fileStore/Mode_example_file.csv"

# Define the schema for the CSV data
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True)
])

#try:
    # Read the CSV file with FAILFAST mode and the specified schema
df = spark.read.option("header", "true") \
                   .option("mode", "FAILFAST") \
                   .schema(schema) \
                   .csv(csv_file_path)

    # Show the DataFrame
df.show()

#except Exception as e:
#    print(f"Exception encountered: {e}")



# COMMAND ----------

# CSV file path
csv_file_path = "/dbfs/fileStore/Mode_example_file.csv"

# Define the schema for the CSV data
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age",  FloatType(), False),
    StructField("country", StringType(), True)
])

# Read the CSV file with PERMISSIVE mode and columnNameOfCorruptRecord option
df = spark.read.schema(schema).option("enforceSchema", "false").option("columnNameOfCorruptRecord", "_corrupt_record")\
               .option("mode", "PERMISSIVE").option("header", "true").csv(csv_file_path)

# Show the DataFrame schema
df.printSchema()

# Show the DataFrame
df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

# Create a SparkSession
spark = SparkSession.builder.appName("Corrupt Records DataFrame").getOrCreate()

# Sample data with some intentionally corrupt records
data = [
    ("John", 25, "USA"),
    ("Alice", 32, "Canada"),
    ("Robert", "forty", "England"),  # Corrupt record with "forty" as age
    ("Mary", 28, "Germany"),
    ("Michael", 35, "France"),
    ("Julia", "invalid", "Russia"),  # Corrupt record with "invalid" as age
    ("David", 40, "Italy"),
    ("Sarah", "null", "Australia"),  # Corrupt record with "null" as age
]

# Define the schema for the DataFrame
columns = ["name", "age", "country"]

# Create the DataFrame
df = spark.createDataFrame(data, columns)

# Add a new column "_corrupt_record" that captures the corrupt records
df = df.withColumn("_corrupt_record", lit(None).cast("string")).filter(col("age").cast("int").isNull())

# CSV file path with corrupt records
csv_file_path = "/dbfs/fileStore/corrupt_records.csv"

# Save the DataFrame to a CSV file using dbutils.fs.put
df.write.option("header", "true").mode('overwrite').csv(csv_file_path)

# Read the CSV file with PERMISSIVE mode and columnNameOfCorruptRecord option
df_read = spark.read \
    .option("header", "true") \
    .option("enforceSchema", "true")\
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .csv(csv_file_path)

# Show the DataFrame schema
df_read.printSchema()

# Show the DataFrame with corrupt records in the _corrupt_record column
df_read.show(truncate=False)


# COMMAND ----------

# CSV file path
csv_file_path = "/dbfs/fileStore/Mode_example_file.csv"

# Define the schema for the CSV data
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

#try:
    # Read the CSV file with FAILFAST mode and the specified schema
test = spark.read.option("header", "true") \
                   .option("mode", "PERMISSIVE") \
                   .schema(schema) \
                   .option("enforceSchema", "false")\
                   .option("columnNameOfCorruptRecord", "_corrupt_record")\
                   .csv(csv_file_path)


# COMMAND ----------

df.show()

# COMMAND ----------

test.columns

# COMMAND ----------

test.dtypes

# COMMAND ----------

test.display()

# COMMAND ----------

df.select("name" , "age").display()

# COMMAND ----------

test.select(test.name , test.age).display()
df.select("name" , "age").display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

test.select(col('name') , col('age')).display()

test.select(test.name , test.age).display()

df.select("name" , "age").display()


# COMMAND ----------


