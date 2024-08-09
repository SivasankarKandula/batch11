# Databricks notebook source
df = spark.read.format('csv').option().mode().load('/path/test.csv')

# COMMAND ----------

df.write.format('csv').mode('error').save('path/test.csv') ---10

# COMMAND ----------

df.write.format('csv').mode('append').save('path/test.csv') ---10 +20 =30

# COMMAND ----------

df.write.format('csv').mode('overwrite').save('path/test.csv') ---20 =20
