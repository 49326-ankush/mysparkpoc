import re

from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[*]").appName("sparkdf").getOrCreate()

data="D:\\all_big_data_sw\\datasets-20220824T114421Z-001\\datasets\\10000Records.csv"
df=spark.read.format("csv").option("header","true").option("sep",",").option("inferSchema","true").load(data)

#res=df.groupby(col("region")).count()
#res.show()
num=int(df.count())
cols=[re.sub('[^a-zA-Z0-9]',"",c)for c in df.columns]
#re is used to replace ... except all small letter,capital letter except all numbers replace them with null.
ndf=df.toDF(*cols)
#toDF is used to rename the all columns, and convert rdd to dataframe.

ndf.printSchema()
#dataframe columns and its datatypes display properly

#data processing programming friendaly (dataframe API)
res=ndf.groupby(col("gender")).agg(count(col("*")).alias("cnt"))
res.show(truncate=False)
#res.show(num,truncate=True) #to see the complit data
#ndf.show(truncate=False)


#df.show(21,truncate=False)
#bydefault showing the top 20 rows and if any moree than 20 charecters its shows the (....) and if you do the truncate=False then u can see the full length data
#df.printSchema()