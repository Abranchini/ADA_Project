
import pyspark
import os
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp,desc, col, count, min, max, sum, asc, coalesce, udf, when, window, explode, unix_timestamp, lit
from pyspark.sql.types import FloatType

spark = SparkSession.builder.getOrCreate()


##### TYPES HAVE CHANGED FOR NumArticles, NumMentions & NumSources

GKG_SCHEMA = StructType([
        StructField("GKGRECORDID",StringType(),True),
        StructField("DATE",StringType(),True),
        StructField("SourceCollectionIdentifier",StringType(),True),
        StructField("SourceCommonName",StringType(),True),
        StructField("DocumentIdentifier",StringType(),True),
        StructField("Counts",StringType(),True),
        StructField("V2Counts",StringType(),True),
        StructField("Themes",StringType(),True),
        StructField("V2Themes",StringType(),True),
        StructField("Locations",StringType(),True),
        StructField("V2Locations",StringType(),True),
        StructField("Persons",StringType(),True),
        StructField("V2Persons",StringType(),True),
        StructField("Organizations",StringType(),True),
        StructField("V2Organizations",StringType(),True),
        StructField("V2Tone",StringType(),True),
        StructField("Dates",StringType(),True),
        StructField("GCAM",StringType(),True),
        StructField("SharingImage",StringType(),True),
        StructField("RelatedImages",StringType(),True),
        StructField("SocialImageEmbeds",StringType(),True),
        StructField("SocialVideoEmbeds",StringType(),True),
        StructField("Quotations",StringType(),True),
        StructField("AllNames",StringType(),True),
        StructField("Amounts",StringType(),True),
        StructField("TranslationInfo",StringType(),True),
        StructField("Extras",StringType(),True)
        ])

EVENTS_SCHEMA = StructType([
    StructField("GLOBALEVENTID",LongType(),True),
    StructField("Day_DATE",LongType(),True),
    StructField("MonthYear_Date",StringType(),True),
    StructField("Year_Date",StringType(),True),
    StructField("FractionDate",FloatType(),True),
    StructField("Actor1Code",StringType(),True),
    StructField("Actor1Name",StringType(),True),
    StructField("Actor1CountryCode",StringType(),True),
    StructField("Actor1KnownGroupCode",StringType(),True),
    StructField("Actor1EthnicCode",StringType(),True),
    StructField("Actor1Religion1Code",StringType(),True),
    StructField("Actor1Religion2Code",StringType(),True),
    StructField("Actor1Type1Code",StringType(),True),
    StructField("Actor1Type2Code",StringType(),True),
    StructField("Actor1Type3Code",StringType(),True),
    StructField("Actor2Code",StringType(),True),
    StructField("Actor2Name",StringType(),True),
    StructField("Actor2CountryCode",StringType(),True),
    StructField("Actor2KnownGroupCode",StringType(),True),
    StructField("Actor2EthnicCode",StringType(),True),
    StructField("Actor2Religion1Code",StringType(),True),
    StructField("Actor2Religion2Code",StringType(),True),
    StructField("Actor2Type1Code",StringType(),True),
    StructField("Actor2Type2Code",StringType(),True),
    StructField("Actor2Type3Code",StringType(),True),
    StructField("IsRootEvent",LongType(),True),
    StructField("EventCode",StringType(),True),
    StructField("EventBaseCode",StringType(),True),
    StructField("EventRootCode",StringType(),True),
    StructField("QuadClass",LongType(),True),
    StructField("GoldsteinScale",FloatType(),True),
    StructField("NumMentions",FloatType(),True),
    StructField("NumSources",FloatType(),True),
    StructField("NumArticles",FloatType(),True),
    StructField("AvgTone",FloatType(),True),
    StructField("Actor1Geo_Type",LongType(),True),
    StructField("Actor1Geo_FullName",StringType(),True),
    StructField("Actor1Geo_CountryCode",StringType(),True),
    StructField("Actor1Geo_ADM1Code",StringType(),True),
    StructField("Actor1Geo_ADM2Code",StringType(),True),
    StructField("Actor1Geo_Lat",FloatType(),True),
    StructField("Actor1Geo_Long",FloatType(),True),
    StructField("Actor1Geo_FeatureID",StringType(),True),
    StructField("Actor2Geo_Type",LongType(),True),
    StructField("Actor2Geo_FullName",StringType(),True),
    StructField("Actor2Geo_CountryCode",StringType(),True),
    StructField("Actor2Geo_ADM1Code",StringType(),True),
    StructField("Actor2Geo_ADM2Code",StringType(),True),
    StructField("Actor2Geo_Lat",FloatType(),True),
    StructField("Actor2Geo_Long",FloatType(),True),
    StructField("Actor2Geo_FeatureID",StringType(),True),
    StructField("ActionGeo_Type",LongType(),True),
    StructField("ActionGeo_FullName",StringType(),True),
    StructField("ActionGeo_CountryCode",StringType(),True),
    StructField("ActionGeo_ADM1Code",StringType(),True),
    StructField("ActionGeo_ADM2Code",StringType(),True),
    StructField("ActionGeo_Lat",FloatType(),True),
    StructField("ActionGeo_Long",FloatType(),True),
    StructField("ActionGeo_FeatureID",StringType(),True),
    StructField("DATEADDED",LongType(),True),
    StructField("SOURCEURL",StringType(),True)
    ])

MENTIONS_SCHEMA = StructType([
    StructField("GLOBALEVENTID",LongType(),True),
    StructField("EventTimeDate",LongType(),True),
    StructField("MentionTimeDate",LongType(),True),
    StructField("MentionType",LongType(),True),
    StructField("MentionSourceName",StringType(),True),
    StructField("MentionIdentifier",StringType(),True),
    StructField("SentenceID",LongType(),True),
    StructField("Actor1CharOffset",LongType(),True),
    StructField("Actor2CharOffset",LongType(),True),
    StructField("ActionCharOffset",LongType(),True),
    StructField("InRawText",LongType(),True),
    StructField("Confidence",LongType(),True),
    StructField("MentionDocLen",LongType(),True),
    StructField("MentionDocTone",FloatType(),True),
    StructField("MentionDocTranslationInfo",StringType(),True),
    StructField("Extras",StringType(),True)
    ])


# cluster directory
DATA_DIR = 'hdfs:///datasets/gdeltv2'
DATA_LOCAL = ''

events_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.export.CSV"),schema=EVENTS_SCHEMA)
mentions_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.mentions.CSV"),schema=MENTIONS_SCHEMA)

############################### Question 0 ##############################################
###### get the number of events grouped by the country they occurred

# the following is hashed because it has been completed
#events_1 = events_df.select('ActionGeo_CountryCode').filter(events_df.ActionGeo_CountryCode.isNotNull())
#CountryEvent_number = events_1.groupBy('ActionGeo_CountryCode').count()

#CountryEvent_number.write.mode("overwrite").parquet('/tmp/msadler/CountryEvent_number')


###### get the number of events grouped by the country reporting

# url to country dataset
Domains = spark.read.format("csv").option("header", "true").load("urls.csv")
Domains = Domains.select(Domains['alpha-2'].alias('code') ,Domains['name'].alias('country_source'), 'region', Domains['web'].alias('url'))


# select the required data from Mentions Dataset
mentions_1 = mentions_df.select("GLOBALEVENTID", "MentionType", "MentionSourceName") \
                .filter(mentions_df["MentionType"] == '1')

# join the domain df with the country corresponding to the source
mentions_2 = mentions_1.join(Domains, Domains['url'] == mentions_1['MentionSourceName'], "left_outer")

# filter out urls that are unknown
mentions_3 = mentions_2.filter(mentions_2.country_source.isNotNull())

events_1 = events_df.select('GLOBALEVENTID')


mention_event = events_1.join(mentions_3, 'GLOBALEVENTID')
CountrySource_number = mention_event.groupBy('country_source').count()
CountrySource_number.write.mode("overwrite").parquet('/tmp/msadler/CountrySource_number')

##### min and max date a reported event happened in the analysed period
events_1 = events_df.select('Day_DATE')

minmax_event_happened = events_1.agg(min("Day_DATE"), max("Day_DATE"))
minmax_event_happened.write.mode("overwrite").parquet('/tmp/msadler/minmax_event_happened')

# min and max date an event was reported
mentions_1 = mentions_df.select('MentionTimeDate')
minmax_date_reported = mentions_1.agg(min("MentionTimeDate"), max("MentionTimeDate"))
minmax_date_reported.write.mode("overwrite").parquet('/tmp/msadler/minmax_date_reported')
