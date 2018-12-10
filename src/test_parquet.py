
import pyspark
import os
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp,desc, col, count, min, max, sum, asc, coalesce, udf, when, window, explode, unix_timestamp, lit
from pyspark.sql.types import FloatType

spark = SparkSession.builder.getOrCreate()



#DATA_LOCAL = 'hdfs:///home/msadler'
Domains = spark.read.format("csv").option("header", "true").load("urls.csv")

##Domains = spark.read.option("sep", "\t").csv(os.path.join(DATA_LOCAL, "urls.csv"),schema=URL_SCHEMA)
#Domains = Domains.select(Domains['alpha-2'].alias('code'), Domains['name'].alias('country_source'), 'region', Domains['web'].alias('url'))
#Domains.show()
