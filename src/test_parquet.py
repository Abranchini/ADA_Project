
import pyspark
import os
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp,desc, col, count, min, max, sum, asc, coalesce, udf, when, window, explode, unix_timestamp, lit
from pyspark.sql.types import FloatType

spark = SparkSession.builder.getOrCreate()


Domains = spark.read.format("csv").option("header", "true").load("hdfs://urls.csv")
Domains = Domains.select(Domains['alpha-2'].alias('code') ,Domains['name'].alias('country_source'), 'region', Domains['web'].alias('url'))
Domains.show()
