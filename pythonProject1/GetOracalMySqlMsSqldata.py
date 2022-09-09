from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
sc.setLogLevel("ERROR")
host="jdbc:mysql://sravanthidb.c7nqndsntouw.us-east-1.rds.amazonaws.com:3306/sravanthidb?useSSL=false"
uname="myuser"
pwd="mypassword"
qry="(select * from emp) t"
df=spark.read.format("jdbc")\
    .option("url",host)\
    .option("dbtable",qry)\
    .option("user",uname)\
    .option("password",pwd)\
    .option("driver","com.mysql.jdbc.Driver").load()

df.show()

'''
#process data

res=df.na.fill(0,['comm','mgr']).withColumn("comm",col("comm").cast(IntegerType()))\
    .withColumn("hiredate",to_date(df.hiredate,"yyyy/MMM/dd"))\
    .withColumn("hiredate1",date_format(col("hiredate"),"yyyy/MMM/dd"))

res.write.format("jdbc").option("url",host)\
    .option("dbtable","emp102")\
    .option("user",uname)\
    .option("password",pwd)\
    .option("driver","com.mysql.jdbc.Driver").save()


res.printSchema() '''