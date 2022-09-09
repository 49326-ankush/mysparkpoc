import sys
import os
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").config("spark.sql.session","EST").getOrCreate()

#data="D:\\all_big_data_sw\\datasets-20220824T114421Z-001\\datasets\\donations.csv"

data=sys.argv[1]
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

res=df.withColumn("dt",to_date(df.dt,"d-M-yyyy")).withColumn("today",current_date()).withColumn("dtdiff",datediff(col("today"),col("dt")))

res.show(truncate=False)