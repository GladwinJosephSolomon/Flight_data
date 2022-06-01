# Databricks notebook source
import pyspark
from pyspark import SparkConf, SparkContext

# COMMAND ----------

conf = SparkConf().setAppName("Flight data")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

df = spark.read.options(inferSchema='True', header='True', delimiter=',').csv('s3://flightdata-cdb/2007.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

#Finding the most frequent tail number which is in destination by maximum and not 0
df1=df.filter((df.TailNum!='0') & (df.TailNum!='000000')) 
df1.groupBy("Dest", "TailNum").count().orderBy("count", ascending=False).show(1)

# COMMAND ----------

#Finding out the cancelled flight details for the last quarter of the year 2007
df.filter((df.Year==2007) & (df.Month>=10) & (df.Cancelled==1)).select('FlightNum', 'Year', 'Month','Cancelled').show()

# COMMAND ----------

#Finding out the average weather delays for a particular flight per month
df.groupBy(df.FlightNum, df.Month).agg({'WeatherDelay': 'avg'}).orderBy(df.FlightNum, df.Month).show()

# COMMAND ----------

#Inspite of NASDelay, SecurityDelay, LateAircraftDelay,Weatherdelay which flight reached exactly on time
from pyspark.sql.types import IntegerType
df2 = df.withColumn("ArrDelay", df["ArrDelay"].cast(IntegerType()))
df2.filter((df.NASDelay>0) & (df.SecurityDelay>0) & (df.LateAircraftDelay>0) & (df.WeatherDelay>0) & (df.ArrDelay<=0)).select('FlightNum','ArrDelay', 'WeatherDelay', 'NASDelay', 'SecurityDelay','LateAircraftDelay','Month', 'Year').show()

# COMMAND ----------

#Month wise total distance travelled by each flight number
df.groupBy(df.FlightNum,df.Month).agg({'Distance': 'sum'}).withColumnRenamed('sum(Distance)','total_distance').show()

# COMMAND ----------

#Month wise how many flights are getting diverted (origin to destination)
df.filter(df.Diverted==1).select('Month','Diverted').groupBy('Month').agg({'Diverted': 'sum'}).withColumnRenamed('sum(Diverted)','total_diverted_flights').show()


# COMMAND ----------

#Week and month wise number of trips in all the flights
df.select('FlightNum','Month','DayOfWeek').groupBy(df.Month,df.DayOfWeek).agg({'FlightNum':'count'}).withColumnRenamed('count(FlightNum)','total_flights').orderBy('Month','DayOfWeek').show()

# COMMAND ----------

#Which flights covered maximum origin and destination by month wise
from pyspark.sql.functions import countDistinct, desc
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
new_df = df.filter(df.Cancelled == 0).groupBy('Month', 'FlightNum').agg(countDistinct('Origin').alias("Max_Origins")).sort('Month', desc('Max_Origins'))
windowDept = Window.partitionBy('Month').orderBy(col("Max_Origins").desc())
new_df.withColumn("row",row_number().over(windowDept)).filter(col("row") == 1).drop("row").show()

# COMMAND ----------

#Average month wise arrival delay (flightnum wise)
import pyspark.sql.functions as func
df.select('FlightNum','Month','ArrDelay').groupBy('FlightNum','Month').agg(func.avg('ArrDelay').alias('average_arrival_delay')).orderBy('FlightNum','Month').show()

# COMMAND ----------

#Average month wise departure delay (flightnum wise)
df.select('FlightNum','Month','DepDelay').groupBy('FlightNum','Month').agg(func.avg('DepDelay').alias('average_deparatur_delay')).show()

# COMMAND ----------


