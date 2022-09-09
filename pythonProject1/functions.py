from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

data="D:\\all_big_data_sw\\drivers-20220824T114220Z-001\\drivers\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","True").load(data)

#ndf=df.groupby(df.state).agg(count("*").alias("cnt")).orderBy(col("cnt").desc())


'''ndf=df.withColumn("fullname",concat_ws(" ",df.first_name,df.last_name))\
    .withColumn("phone1",regexp_replace(col("phone1"),"-","").cast(LongType()))\
    .withColumn("phone2",regexp_replace(col("phone2"),"-","").cast(LongType()))\
    .select(col("first_name"),col("last_name"),col("fullname"),col("phone1"),col("phone2"))\
    .withColumnRenamed("first_name","f_name")\
    .withColumnRenamed("last_name","l_name")
'''
#cast() is used to converting the data in to appropreate formate.
#with columns is user to add new columns(if columns is not exist) or update columns(if alredy exist)
#lit(value) used to add something dummy value it.
#drop will delet the unnecessory columns.
#withColumnsRenamed() it is used to rename the columns at a time.

#ndf=df.groupby(df.state).agg(count("*").alias("cnt"),collect_list(df.city)).orderBy(col("cnt").desc())
#ndf=df.groupby(df.state).agg(count("*").alias("cnt"),collect_set(df.city)).orderBy(col("cnt").desc())

#ndf=df.withColumn("state",when(col("state")=="NY","NewYork").when(col("state")=="CA","californiya").otherwise(col("state")))

#ndf=df.withColumn("address1",when(col("address").contains("#"),"*****").otherwise(col("address"))).withColumn("address2",regexp_replace("address","#","*"))

#ndf1=df.withColumn("substr",substring("email",0,5)).withColumn("gmails",substring_index("email","@",-1)).withColumn("usernames",substring_index(col("email"),"@",1)).drop("first_name","last_name","address","city","country","phone1","phone2","web")

#ndf=ndf1.groupby(col("gmails")).count().orderBy(col("count").desc())

#sql friendaly, same operation in sql
df.createOrReplaceTempView("tab")
#ndf=spark.sql("select *,concat_ws(' ',first_name,last_name) fullname from tab")
#ndf=spark.sql("select *,regexp_replace(phone1,'-','#') ph from tab")
#ndf=spark.sql("select *,when(state==NY,'***') whet from tab")
qry="""with tmp as (select *,substring_index(email,'@',-1) mail from tab)
 select mail,count(*) cnt from tmp group by mail order by cnt desc """

ndf=spark.sql(qry)
ndf.show()

#ndf.printSchema()

