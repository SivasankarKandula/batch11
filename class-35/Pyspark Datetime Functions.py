# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------


df = spark.createDataFrame([('2015-04-08', 2)], ['dt', 'add'])
df.printSchema
df.show()

# COMMAND ----------


df.select(add_months(df.dt, 12).alias('next_month')).show()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.select(add_months(df.dt, df.add.cast('integer')).alias('next_month')).show()

# COMMAND ----------



df.select(add_months(df.dt, df.add.cast('integer')).alias('next_month')).show()


# COMMAND ----------


df2= df.select('*', current_date())
df2.show()
df2.printSchema()

# COMMAND ----------

df.select('*', current_timestamp()).show(truncate = False)

# COMMAND ----------

#pyspark.sql.functions.date_add
#pyspark.sql.functions.date_add(start: ColumnOrName, days: Union[ColumnOrName, int]) → pyspark.sql.column.Colum

#Returns the date that is days days after start
from pyspark.sql.functions import date_add
df = spark.createDataFrame([('2015-04-08', 2,)], ['dt', 'add'])

# COMMAND ----------

df.show()

# COMMAND ----------




df.select(date_add(df.dt, 35).alias('next_date')).show()

# COMMAND ----------



df.select(date_add(df.dt, df.add.cast('integer')).alias('next_date')).show()


# COMMAND ----------

#pyspark.sql.functions.date_format
#pyspark.sql.functions.date_format(date: ColumnOrName, format: str) → pyspark.sql.column.Column


#Converts a date/timestamp/string to a value of string in the format specified by the date format given by the second argument.
#A pattern could be for instance dd.MM.yyyy and could return a string like ‘18.03.1993’. All pattern letters of datetime pattern. can be used.

#Syntax:  date_format(column,format)
#Example: date_format(current_timestamp(),"yyyy MM dd").alias("date_format")

from pyspark.sql.functions import date_format

df = spark.createDataFrame([('2015-04-08',)], ['dt'])
df.show()

# COMMAND ----------


df1 = df.select(date_format('dt', 'MM/dd/yyy').alias('date'))
df1.show()


# COMMAND ----------

from pyspark.sql.functions import *

df=spark.createDataFrame([["1"]],["id"])
df.select(current_date().alias("current_date"), \
      current_timestamp().alias("current_timestamp"),\
      date_format(current_timestamp(),"yyyy MM dd").alias("yyyy MM dd"), \
      date_format(current_timestamp(),"MM/dd/yyyy hh:mm").alias("MM/dd/yyyy"), \
      date_format(current_timestamp(),"yyyy MMM dd").alias("yyyy MMMM dd"), \
      date_format(current_timestamp(),"yyyy MMMM dd E").alias("yyyy MMMM dd E") \
   ).display()




# COMMAND ----------

df.createOrReplaceTempView("t1")

# COMMAND ----------

#sql

spark.sql("select current_date() as current_date, " +
      "date_format(current_timestamp(),'yyyy MM dd') as yyyy_MM_dd, "+
      "date_format(current_timestamp(),'MM/dd/yyyy hh:mm') as MM_dd_yyyy, "+
      "date_format(current_timestamp(),'yyyy MMM dd') as yyyy_MMMM_dd, "+
      "date_format(current_timestamp(),'yyyy MMMM dd E') as yyyy_MMMM_dd_E").show()


# COMMAND ----------

#pyspark.sql.functions.date_sub
#pyspark.sql.functions.date_sub(start: ColumnOrName, days: Union[ColumnOrName, int]) → pyspark.sql.column.Column

#Returns the date that is days days before start

df = spark.createDataFrame([('2024-04-08', 2,)], ['dt', 'sub'])
df.select(date_sub(df.dt, 1).alias('prev_date')).show()

df.select(date_sub(df.dt, df.sub.cast('integer')).alias('prev_date')).show()

# COMMAND ----------

#pyspark.sql.functions.date_trunc
#pyspark.sql.functions.date_trunc(format: str, timestamp: ColumnOrName) → pyspark.sql.column.Column

#Returns timestamp truncated to the unit specified by the format.

#Parameters:  format: str:
 # ‘year’, ‘yyyy’, ‘yy’ to truncate by year, ‘month’, ‘mon’, ‘mm’ to truncate by month, ‘day’, ‘dd’ to truncate by day, Other options are: ‘microsecond’, ‘millisecond’,       #  ‘second’, ‘minute’, ‘hour’, ‘week’, ‘quarter’

# timestamp:  Column or str


df = spark.createDataFrame([('1997-02-28 05:02:11',)], ['t'])


# COMMAND ----------

1997-02-04: 00:00:00

# COMMAND ----------

df.display()

# COMMAND ----------

df.select(df.t, date_trunc('year', df.t).alias('year')).display()

# COMMAND ----------

df.select(df.t, date_trunc('mon', df.t).alias('month')).show()

# COMMAND ----------




df.select(df.t,date_trunc('day', df.t).alias('day')).show()

# COMMAND ----------

#pyspark.sql.functions.datediff
#pyspark.sql.functions.datediff(end: ColumnOrName, start: ColumnOrName) → pyspark.sql.column.Column
#Returns the number of days from start to end.

df = spark.createDataFrame([('2015-04-08','2015-05-10')], ['d1', 'd2'])
df.select(datediff(df.d2, df.d1).alias('diff')).show()
df.select(datediff(df.d1, df.d2).alias('diff')).show()

# COMMAND ----------

#pyspark.sql.functions.dayofmonth
#pyspark.sql.functions.dayofmonth(col: ColumnOrName) → pyspark.sql.column.Column
#Extract the day of the month of a given date as integer.

df = spark.createDataFrame([('2024-04-28',)], ['dt'])
df.select(dayofmonth('dt').alias('day')).show()

# COMMAND ----------

#pyspark.sql.functions.dayofweek
#pyspark.sql.functions.dayofweek(col: ColumnOrName) → pyspark.sql.column.Column
#Extract the day of the week of a given date as integer. Ranges from 1 for a Sunday through to 7 for a Saturday.

df = spark.createDataFrame([('2024-07-29',)], ['dt'])
df.select(dayofweek('dt').alias('day')).show()

# COMMAND ----------

#pyspark.sql.functions.dayofyear
#pyspark.sql.functions.dayofyear(col: ColumnOrName) → pyspark.sql.column.Column
#Extract the day of the year of a given date as integer

df = spark.createDataFrame([('2015-02-08',)], ['dt'])
df.select(dayofyear('dt').alias('day')).show()

# COMMAND ----------

#pyspark.sql.functions.weekofyear
#pyspark.sql.functions.weekofyear(col: ColumnOrName) → pyspark.sql.column.Column

#Extract the week number of a given date as integer. A week is considered to start on a Monday and week 1 is the first week with more than 3 days, as defined by ISO 8601

from pyspark.sql.functions import *

df = spark.createDataFrame([('2015-02-01',)], ['dt'])
df.select(weekofyear(df.dt).alias('week')).show()

# COMMAND ----------

#pyspark.sql.functions.year
#pyspark.sql.functions.year(col: ColumnOrName) → pyspark.sql.column.Column
#Extract the year of a given date as integer

df = spark.createDataFrame([('2015-04-08',)], ['dt'])
df.select(year('dt').alias('year')).show()

# COMMAND ----------

#pyspark.sql.functions.quarter
#pyspark.sql.functions.quarter(col: ColumnOrName) → pyspark.sql.column.Column
#Extract the quarter of a given date as integer.

df = spark.createDataFrame([('2015-10-08',)], ['dt'])
df.select(quarter('dt').alias('quarter')).show()

# COMMAND ----------

#pyspark.sql.functions.month
#pyspark.sql.functions.month(col: ColumnOrName) → pyspark.sql.column.Column

df = spark.createDataFrame([('2015-04-08',)], ['dt'])
df.select(month('dt').alias('month')).show()

# COMMAND ----------

#pyspark.sql.functions.last_day
#pyspark.sql.functions.last_day(date: ColumnOrName) → pyspark.sql.column.Column
#Returns the last day of the month which the given date belongs to.

df = spark.createDataFrame([('2023-02-10',)], ['d'])
df.select(last_day(df.d).alias('date')).show()

# COMMAND ----------

#pyspark.sql.functions.minute
#pyspark.sql.functions.minute(col: ColumnOrName) → pyspark.sql.column.Column

#Extract the minutes of a given date as integer.

import datetime
df = spark.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ['ts'])
df.select(minute('ts').alias('minute')).show()

# COMMAND ----------

#pyspark.sql.functions.months_between
#pyspark.sql.functions.months_between(date1: ColumnOrName, date2: ColumnOrName, roundOff: bool = True) → pyspark.sql.column.Column

#Returns number of months between dates date1 and date2. If date1 is later than date2, then the result is positive. A whole number is returned if both inputs have the same day of month or both are the last day of their respective months. Otherwise, the difference is calculated assuming 31 days per month. The result is rounded off to 8 digits unless roundOff is set to False.


df = spark.createDataFrame([('2023-07-01 10:30:00', '2023-08-01')], ['date1', 'date2'])
df.select(months_between(df.date2, df.date1).alias('months')).show()

df.select(months_between(df.date1, df.date2, False).alias('months')).show()

# COMMAND ----------

#pyspark.sql.functions.next_day
#pyspark.sql.functions.next_day(date: ColumnOrName, dayOfWeek: str) → pyspark.sql.column.Column

#Returns the first date which is later than the value of the date column.

#Day of the week parameter is case insensitive, and accepts:
#“Mon”, “Tue”, “Wed”, “Thu”, “Fri”, “Sat”, “Sun”.

df = spark.createDataFrame([('2024-07-28',)], ['d'])
df.select(next_day(df.d, 'Mon').alias('date')).show()


# COMMAND ----------

#pyspark.sql.functions.hour
#pyspark.sql.functions.hour(col: ColumnOrName) → pyspark.sql.column.Column

#Extract the hours of a given date as integer.

import datetime
df = spark.createDataFrame([(datetime.datetime(2015, 4, 8, 13, 8, 15),)], ['ts'])
df.select(hour('ts').alias('hour')).show()

# COMMAND ----------

#pyspark.sql.functions.make_date
#pyspark.sql.functions.make_date(year: ColumnOrName, month: ColumnOrName, day: ColumnOrName) → pyspark.sql.column.Column[source]
# Returns a column with a date built from the year, month and day columns.

#Parameters: year : Column or str   ---The year to build the date
#            month: Column or str   ---The month to build the date
#            day  : Column or str   ---The day to build the date
        
df = spark.createDataFrame([(2020, 6, 26)], ['Y', 'M', 'D'])
df.select(make_date(df.Y, df.M, df.D).alias("datefield")).show()

# COMMAND ----------

#pyspark.sql.functions.to_timestamp(col: ColumnOrName, format: Optional[str] = None) → pyspark.sql.column.Column

#Converts a Column into pyspark.sql.types.TimestampType using the optionally specified format. Specify formats according to datetime pattern. By default, it follows casting rules to pyspark.sql.types.TimestampType if the format is omitted. Equivalent to col.cast("timestamp").

df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
df.dtypes
df2 = df.select(to_timestamp(df.t).alias('dt'))
#df2.show()
#df2.dtypes

# COMMAND ----------

#pyspark.sql.functions.to_date
#pyspark.sql.functions.to_date(col: ColumnOrName, format: Optional[str] = None) → pyspark.sql.column.Column

#Converts a Column into pyspark.sql.types.DateType using the optionally specified format. Specify formats according to datetime pattern. By default, it follows casting rules to pyspark.sql.types.DateType if the format is omitted. Equivalent to col.cast("date").

df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])
df.select(to_date(df.t).alias('date')).show()
df = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])

df.select(to_date(df.t, 'yyyy-MM-dd HH:mm:ss').alias('date')).show()



# COMMAND ----------

df=spark.createDataFrame(
data = [ ("1","2019-06-24 12:01:19.000")],
        schema=["id","input_timestamp"])
df.printSchema()

from pyspark.sql.functions import *

#Timestamp String to DateType
df.withColumn("date_type",to_date("input_timestamp")) \
  .show(truncate=False)

#Timestamp Type to DateType
df.withColumn("date_type",to_date(current_timestamp())) \
  .show(truncate=False) 

#Custom Timestamp format to DateType
df.select(to_date(lit('06-24-2019 12:01:19.000'),'MM-dd-yyyy HH:mm:ss.SSSS')) \
  .show()


# COMMAND ----------

#Convert TimestampType (timestamp) to DateType (date)
#Timestamp type to DateType
df.withColumn("ts",to_timestamp(col("input_timestamp"))) \
  .withColumn("datetype",to_date(col("ts"))) \
  .show(truncate=False)

# COMMAND ----------

# Using Cast to convert Timestamp String to DateType
df.withColumn('date_type', col('input_timestamp').cast('date')) \
       .show(truncate=False)

# Using Cast to convert TimestampType to DateType
df.withColumn('date_type', to_timestamp('input_timestamp').cast('date')) \
  .show(truncate=False)

# COMMAND ----------

#PySpark SQL – Convert Timestamp to Date
#SQL TimestampType to DateType
spark.sql("select to_date(current_timestamp) as date_type")

#SQL CAST TimestampType to DateType
spark.sql("select date(to_timestamp('2019-06-24 12:01:19.000')) as date_type")

#SQL CAST timestamp string to DateType
spark.sql("select date('2019-06-24 12:01:19.000') as date_type")

#SQL Timestamp String (default format) to DateType
spark.sql("select to_date('2019-06-24 12:01:19.000') as date_type")

#SQL Custom Timeformat to DateType
spark.sql("select to_date('06-24-2019 12:01:19.000','MM-dd-yyyy HH:mm:ss.SSSS') as date_type")
