from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("sparkdf").getOrCreate()

#used case 1

#data="D:\\all_big_data_sw\\datasets-20220824T114421Z-001\\datasets\\donations.csv"
#df=spark.read.format("csv").option("header","true").load(data)
#when we reading the csv data in dataframe the header is bydefault is false so it will
# not take the first line of the data as name of columns
#if we mension header true it will take the first line of data as the name of columns.

#used case 2

#if u have any mal record like first line is like some instruction and may be econd line wrong then cleand that data using rdd or udf.
#this program meaning the skip the first line, take second line as header and data from thered line.

data="D:\\all_big_data_sw\\datasets-20220824T114421Z-001\\datasets\\donations1.csv"
rdd=spark.sparkContext.textFile(data)
skip=rdd.first()
odata=rdd.filter(lambda x:x!=skip)
df=spark.read.csv(odata,header=True,inferSchema=True)
df.printSchema() #priting the schema in nice tree format
df.show(5) #disply the top 20 records and if u want to desply the top 5 line the we can just used df.show(5).

