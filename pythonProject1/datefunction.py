from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").config("spark.sql.session.timezone","EST").getOrCreate()

data="D:\\all_big_data_sw\\datasets-20220824T114421Z-001\\datasets\\donations.csv"

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
#spark bydefalt able to understand 'yyy-mm-dd' formate only

def nums(days):
    yr = days % 365
    year = int(days / 365)
    mon = yr % 30
    month = int(yr / 30)
    dy = mon
    result = year, "year", month, "month", dy, "days"
    st = ''.join(map(str, result))
    return st
udffun=udf(nums)


#to_date function convert the inpute date formate in 'yyyy-mm-dd' format.
#current_date function show the current date
#config("spark.sql.session.timezone","EST").. its very imp based on the original clint date all default time based on us time only. at thet time mension EST only.
#current_timestamp function will show the current time stamp.
#date_add is adding the number of day in to the date
#what next sun or monday or friday from today u will get

res=df.withColumn("dt",to_date(df.dt,"d-M-yyyy"))\
    .withColumn("ts",current_timestamp())\
    .withColumn("today",current_date())\
    .withColumn("dtdiff",datediff(col("today"),col("dt")))\
    .withColumn("dtadd",date_add(col("dt"),100))\
    .withColumn("dtsum",date_sub(col("dt"),100))\
    .withColumn("lastdt",last_day(col("dt")))\
    .withColumn("lastdy1",date_format(last_day(col("dt")),"yyyy-MM-dd-EEEE"))\
    .withColumn("nextday",next_day(col("today"),"Mon"))\
    .withColumn("dtformat",date_format(col("dt"),"dd/MMMM/yy/EEEE/zzz")) \
    .withColumn("monlastfri", next_day(date_add(last_day(col("dt")),-7),"Fri"))\
    .withColumn("dayofweek",dayofweek(col("dt")))\
    .withColumn("dayofmon",dayofmonth(col("dt")))\
    .withColumn("dayofyear",dayofyear(col("dt")))\
    .withColumn("yr",year(col("dt")))\
    .withColumn("monbetween",months_between(current_date(),col("dt")))\
    .withColumn("floor",floor(col("monbetween")))\
    .withColumn("ceil",ceil(col("monbetween")))\
    .withColumn("round",round(col("monbetween")).cast(IntegerType()))\
    .withColumn("dttrunc",date_trunc("mon",col("dt")))\
    .withColumn("weekofyear",weekofyear(col("dt")))\
    .withColumn("daystoyrmon",udffun(col("dtdiff")))



#dtdiff is 588 days, i want to convert it into 1yr-3mon-4days like this
#create udf to get the output in expeted formate.


#date_formate used to get ur desire date format ex. 20/April/21/
#datofweek() means the from the sunday how many days complited
#dayofmonth() means from 1 month how many days complited
#dayofyear() means from 1 jan to specified date how namy days complited.

res.show(truncate=False)
#res.printSchema()