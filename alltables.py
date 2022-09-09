from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"D:\\all_big_data_sw\\datasets-20220824T114421Z-001\\datasets\\config.txt")
host=conf.get("cred","host")
uname=conf.get("cred","uname")
pwd=conf.get("cred","pwd")

#tabs=['dept','emp','ankush','test','emp102']
'''
for i in tabs:
    df = spark.read.format("jdbc") \
        .option("url", host) \
        .option("dbtable", i) \
        .option("user", uname) \
        .option("password", pwd) \
        .option("driver", "com.mysql.jdbc.Driver").load()
    df.show()'''

qry="(select * from emp) aaa"

df1=spark.read.format("jdbc").option("url",host).option("user",uname)\
    .option("password",pwd)\
    .option("dbtable",qry)\
    .option("driver","com.mysql.jdbc.Driver")\
    .load()

tabs=[x[0] for x in df1.collect()]

df1.show()