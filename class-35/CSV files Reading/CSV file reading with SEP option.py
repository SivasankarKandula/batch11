# Databricks notebook source
spark.read.format('parquet').option('header' , 'true').load("dbfs:/filestore/t1.csv")


spark.read.csv("dbfs:/filestore/t1.csv" , header=True )

# COMMAND ----------

Name;age;city;occupation 

# COMMAND ----------

df = spark.read.csv("dbfs:/filestore/t1.csv" , header=True)
df.display()

# COMMAND ----------

df = spark.read.csv("dbfs:/filestore/t1.csv", header=True, inferSchema=True)
df.display()

# COMMAND ----------

spark.read.format('csv').option('header' , 'true').option('sep' , ';').load("dbfs:/filestore/t1.csv").display()

# COMMAND ----------

df = spark.read.csv("dbfs:/filestore/t1.csv", header=True ,sep = ';')
df.display()

# COMMAND ----------

# Read the CSV file with the custom separator ";"
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ";") \
    .csv("dbfs:/filestore/t1.csv")

# Show the DataFrame
df.display()


# COMMAND ----------

df.printSchema()

# COMMAND ----------


