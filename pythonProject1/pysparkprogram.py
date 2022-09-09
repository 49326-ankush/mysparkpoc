from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

sc=spark.sparkContext

win=Window.partitionBy('state').orderBy(col("zip").desc())

data="D:\\all_big_data_sw\\datasets-20220824T114421Z-001\\datasets\\us-500.csv"
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
ndf=df.withColumn("ntil",ntile(5).over(win)).withColumn("drank",dense_rank().over(win))
ndf.show()