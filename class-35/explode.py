#siva
# Databricks notebook source
from pyspark.sql.functions import explode, explode_outer, posexplode, posexplode_outer, col, array

# Sample DataFrame
data = [("James", ["Java", "Scala", "C++"], ["Spark", "Java"]),
        ("Michael", ["Spark", "Java", "C++"], []),
        ("Robert", [], ["Snowflake", "Oracle"]),
        ("Washington", None, None)]
columns = ["Name", "KnownLanguages", "Databases"]

# COMMAND ----------


df = spark.createDataFrame(data, schema=columns)

# COMMAND ----------

df.display()

# COMMAND ----------

explode -----------list----one columns

# COMMAND ----------

name knowlangu database
james   java     spark
james   java     java
james   scala    spark
james   scala    java
james   c++     spark
james   c++     java

# COMMAND ----------

# Using explode
df_explode = df.select(df.Name, explode(df.KnownLanguages).alias("Language"),df.Databases )
display(df_explode)

# COMMAND ----------

 df_explode_final= df_explode.select(df_explode.Name, "Language",explode(df_explode.Databases ).alias('database'))
display( df_explode_final)

# COMMAND ----------


# Using explode_outer
df_explode_outer = df.select(df.Name, explode_outer(df.KnownLanguages).alias("Language"),df.Databases)
display(df_explode_outer)

# COMMAND ----------

# Using posexplode
df_posexplode = df.select(df.Name, posexplode(df.KnownLanguages).alias("Position", "Language"))
display(df_posexplode)

# COMMAND ----------



# Using posexplode_outer
df_posexplode_outer = df.select(df.Name, posexplode_outer(df.KnownLanguages).alias("Position", "Language"))
display(df_posexplode_outer)
