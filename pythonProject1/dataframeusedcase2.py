from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("sparkdf").getOrCreate()

data="D:\\all_big_data_sw\\datasets-20220824T114421Z-001\\datasets\\bank-full.csv"
df=spark.read.format("csv").option("header","true").option("sep",";").option("inferSchema","true").load(data)
#sep is used to specify the data seperater

#data processing in programming frindaly

#res=df.where((col("age")>60) & (col("marital")=="married"))
#res=df.select(col("age"),col("marital"),col("balance")).where((col("age")>60) & (col("marital")=="married"))
#select will disply only selected column and if u want to disply the all columns the just ignore the select part.

#res=df.select(col("age"),col("marital"),col("balance")).where(((col("age")>60) | (col("marital")=="married")) & (col("balance")>40000))

#this is programming frindaly
#res=df.groupby(col("marital")).agg(sum(col("balance")).alias("smb")).sort(col("smb"))
#res=df.groupby(col("marital")).count()
#res=df.groupby(col("marital")).agg(count("*").alias("cnt"),sum(col("balance")).alias("smb")).where(col("balance")>=avg(col("balance")))
res=df.groupby(col("marital")).agg(avg(col("balance")).alias("avrg"),count(col("marital")).alias("cont"),sum(col("balance")).alias("smb"))

#this is sql friendaly
df.createOrReplaceTempView("tab")
#createOrreplaceTempView it regester this data frame as table it is very used full to run sql query
#res=spark.sql("select * from tab where age>50 and balance>50000")
#res=spark.sql("select sum(balance) from tab group by marital")

res.show()
#df.printSchema()