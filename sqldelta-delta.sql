-- Databricks notebook source
create table parquet_tablea3dvp1 (eno int , ename varchar(100) , esal int)
using parquet LOCATION 'dbfs:/delta/parquet_tablea3dvp1'

-- COMMAND ----------

create table parquet_tablea3dv (eno int , ename varchar(100) , esal int)
using delta LOCATION 'dbfs:/delta/parquet_tablea3dv'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/delta/parquet_tablea3dvp1')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/delta/parquet_tablea3dv/_delta_log/')

-- COMMAND ----------

describe  parquet_tablea3d

-- COMMAND ----------

describe extended parquet_tablea3d

-- COMMAND ----------

update parquet_tablea3dv set esal = esal +100

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format('json').load('dbfs:/delta/parquet_tablea3dv/_delta_log/00000000000000000005.json')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.display()

-- COMMAND ----------

select *from  parquet_tablea3d

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/delta/')

-- COMMAND ----------

insert into parquet_tablea3dv 
select *from parquet_tablea3dv

-- COMMAND ----------

insert into parquet_tablea3dv values(11,'sankar', 3000) ;
insert into parquet_tablea3dv values(12,'kandula', 5000);
insert into parquet_tablea3dv values(13,'aruna', 8000);
insert into parquet_tablea3dv values(14,'prudhvi', 7000);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/delta/parquet_tablea3dv/_delta_log/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC  spark.read.format('json').load('dbfs:/delta/parquet_tablea3dv/_delta_log/00000000000000000003.json').display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format('delta').load('dbfs:/delta/parquet_tablea3d')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/delta/parquet_tablea3d')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/delta/parquet_tablea3d/_delta_log/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.option('multiline' , 'true').json('dbfs:/delta/parquet_tablea3d/_delta_log/00000000000000000003.json')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.head('dbfs:/delta/parquet_tablea3d/_delta_log/00000000000000000003.json')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.show()

-- COMMAND ----------

delete from parquet_tablea3d where eno = 11

-- COMMAND ----------

update parquet_tablea3d set esal = esal + 100

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC df = df.withColumn('dept' , lit('EEE'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.dtypes

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode('append').option('meregeSchema' , 'true').format('delta').save('dbfs:/delta/parquet_tablea3d')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format('delta').load('dbfs:/delta/parquet_tablea3d')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.show()

-- COMMAND ----------

describe extended delta_table1

-- COMMAND ----------

select *from delta_table1

-- COMMAND ----------

insert into parquet_table2 values(11,'sankar', 3000);
insert into parquet_table2 values(12,'kandula', 5000);
insert into parquet_table2 values(13,'aruna', 8000);
insert into parquet_table2 values(14,'prudhvi', 7000);


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # create first dataframe with schema
-- MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, StringType
-- MAGIC data1 = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
-- MAGIC schema1 = StructType([StructField("name", StringType(), True), StructField("age", IntegerType(), True)])
-- MAGIC df1 = spark.createDataFrame(data1, schema1)
-- MAGIC
-- MAGIC # create second dataframe with different schema
-- MAGIC data2 = [("Dave", 4, 100), ("Eve", 5, 101), ("Frank", 6, 102)]
-- MAGIC schema2 = StructType([StructField("name", StringType(), True), StructField("age", IntegerType(), True), StructField("id", IntegerType(), True)])
-- MAGIC df2 = spark.createDataFrame(data2, schema2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Enforce schema
-- MAGIC df1.write.format('parquet').option('mergeSchema', 'true').mode('append').parquet('/tmp/test.parqueta1')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Enforce evolution
-- MAGIC df1.write.format('parquet').option('mergeSchema', 'false').mode('append').parquet('/tmp/test.parquetb1')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.format('parquet').load('/tmp/test.parquetb1').display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.format('parquet').load('/tmp/test.parqueta1').display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df2.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Enforce schema
-- MAGIC df2.write.format('parquet').option('mergeSchema', 'true').mode('append').parquet('/tmp/test.parqueta1')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Enforce evolutions
-- MAGIC df2.write.format('parquet').option('mergeSchema', 'false').mode('append').parquet('/tmp/test.parquetb1')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.format('parquet').load('/tmp/test.parqueta1').display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.format('parquet').load('/tmp/test.parquetb1').display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # create first dataframe with schema
-- MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, StringType
-- MAGIC data1 = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
-- MAGIC schema1 = StructType([StructField("name", StringType(), True), StructField("age", IntegerType(), True)])
-- MAGIC df3 = spark.createDataFrame(data1, schema1)
-- MAGIC
-- MAGIC # create second dataframe with different schema
-- MAGIC data2 = [("Dave", 4, 100), ("Eve", 5, 101), ("Frank", 6, 102)]
-- MAGIC schema2 = StructType([StructField("name", StringType(), True), StructField("age", IntegerType(), True), StructField("id", IntegerType(), True)])
-- MAGIC df4 = spark.createDataFrame(data2, schema2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # create second dataframe with different schema
-- MAGIC data2 = [("Dave", 100), ("Eve",  101), ("Frank", 102)]
-- MAGIC schema2 = StructType([StructField("name", StringType(), True), StructField("id", IntegerType(), True)])
-- MAGIC df5 = spark.createDataFrame(data2, schema2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Enforce schema
-- MAGIC df3.write.format('parquet').option('mergeSchema', 'false').mode('append').parquet('/tmp/test.parquet6')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.format('parquet').load('/tmp/test.parquet6').display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Enforce schema
-- MAGIC df4.write.format('parquet').option('mergeSchema', 'false').mode('append').parquet('/tmp/test.parquet6')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.format('parquet').load('/tmp/test.parquet6').display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Enforce schema
-- MAGIC df3.write.format('parquet').option('mergeSchema', 'false').mode('append').parquet('/tmp/test.parquet6')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.format('parquet').load('/tmp/test.parquet6').display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Enforce schema
-- MAGIC df5.write.format('parquet').option('mergeSchema', 'true').mode('append').parquet('/tmp/test.parquet6')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.format('parquet').load('/tmp/test.parquet6').display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('/tmp/test.parquet6')

-- COMMAND ----------

select *from parquet_tablea3dv

-- COMMAND ----------

describe history parquet_tablea3dv

-- COMMAND ----------

select *from parquet_tablea3dv timestamp as of '2024-02-08T15:21:10.000+0000'

-- COMMAND ----------

select *from parquet_tablea3dv version as of 6

-- COMMAND ----------


