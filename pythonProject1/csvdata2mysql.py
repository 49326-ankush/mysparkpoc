import re
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"D:\\all_big_data_sw\\datasets-20220824T114421Z-001\\datasets\\config.txt")
host=conf.get("cred","host")
uname=conf.get("cred","uname")
pwd=conf.get("cred","pwd")
#heare we are hiding the credential like password, host name , username like that.
data="D:\\all_big_data_sw\\datasets-20220824T114421Z-001\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("sep",",").option("inferSchema","true").load(data)


cols=[re.sub('[^a-zA-Z0-9]',"",c)for c in df.columns]

ndf=df.toDF(*cols)

ndf.write.mode("overwrite").format("jdbc").option("url",host)\
    .option("dbtable","testankush")\
    .option("user",uname)\
    .option("password",pwd)\
    .option("driver","com.mysql.jdbc.Driver").save()

ndf.show()

ndf.printSchema()