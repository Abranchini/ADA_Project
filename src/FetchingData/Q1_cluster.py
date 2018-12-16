import pyspark
import os
import scipy as sp
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
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
    StructField("Day_DATE",StringType(),True),
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

PRINT_SCHEMA = StructType([StructField("Print_message", StringType(), True),
                      StructField("number",LongType(),True)])

# cluster directory
DATA_DIR = 'hdfs:///datasets/gdeltv2'
DATA_LOCAL = ''

# open all the datasets
gkg_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.gkg.csv"),schema=GKG_SCHEMA)
events_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.export.CSV"),schema=EVENTS_SCHEMA)
mentions_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.mentions.CSV"),schema=MENTIONS_SCHEMA)

#### open helper sets

# url to country dataset
Domains = spark.read.format("csv").option("header", "true").load("urls.csv")
Domains = Domains.select(Domains['alpha-2'].alias('code') ,Domains['name'].alias('country_source'), 'region', Domains['web'].alias('url'))

CountryToCapital = spark.read.format("csv").option("header", "true").load("country-capitals.csv")

############################### Question 1 ##############################################

# select the required data from Mentions Dataset
mentions_1 = mentions_df.select("GLOBALEVENTID", "EventTimeDate", 'MentionIdentifier',"MentionType", "MentionSourceName") \
                .filter(mentions_df["MentionType"] == '1')

# join the domain df with the country corresponding to the source
mentions_2 = mentions_1.join(Domains, Domains['url'] == mentions_1['MentionSourceName'], "left_outer")

# filter out urls that are unknown
mentions_3 = mentions_2.filter(mentions_2.country_source.isNotNull())

# print the number of urls that have no country
#print('number of known urls: ', mentions_3.select('url').distinct().count())

#urls_resume = [(Row('number of unknown urls: ', mentions_2.filter("url is null").select('MentionSourceName').distinct().count())), (Row('number of known urls: ', mentions_3.select('url').distinct().count()))]

#urls_df = spark.createDataFrame(urls_resume,schema=PRINT_SCHEMA)

#urls_df.write.mode("overwrite").parquet('/tmp/msadler/urls_df')

# join the file with the countries and capitals
mentions_4 = mentions_3.join(CountryToCapital, CountryToCapital['CountryName'] == mentions_3['country_source'], "left_outer")
#print('Total number of articles with a country assigned: ', mentions_4.count())
#print('Total number of articles with no country assigned: ', mentions_4.filter("CapitalLatitude is null").count())

#articles_resume = [(Row('Total number of articles with a country assigned:  ', mentions_4.count())), (Row('Total number of articles with no country assigned: ', mentions_4.filter("CapitalLatitude is null").count()))]

#articles_df = spark.createDataFrame(articles_resume,schema=PRINT_SCHEMA)

#articles_df.write.mode("overwrite").parquet('/tmp/msadler/articles_df')

# filter out rows with no geographic coordinates
mentions_5 = mentions_4.filter(mentions_4.CapitalLatitude.isNotNull())

# select relevant columns
mentions_6 = mentions_5.select('GLOBALEVENTID', 'MentionIdentifier', 'EventTimeDate','CountryCode', 'CountryName', mentions_5['CapitalLatitude'].alias('Source_Lat'),
                                                   mentions_5['CapitalLongitude'].alias('Source_Long'))
mentions_6 = mentions_6.withColumn("Source_Lat", mentions_6["Source_Lat"].cast("float"))
mentions_6 = mentions_6.withColumn("Source_Long", mentions_6["Source_Long"].cast("float"))

# select Data from Events Dataset
events_1= events_df.select("GLOBALEVENTID", 'ActionGeo_CountryCode',"ActionGeo_Lat", "ActionGeo_Long", "NumMentions","NumSources","NumArticles","AvgTone")

# filter out events that have no geographic coordinates
#print('Total number of events: ', events_1.count())
events_2 = events_1.filter(events_1.ActionGeo_Lat.isNotNull())
#print('Number of events with geographic coordinates: ', events_2.count())

## filter on the gkg file

# prepare gkg file
gkg_1 = gkg_df.select('DocumentIdentifier', 'V2Tone')
split_col = split(gkg_df['V2Tone'], ',')
gkg_2 = gkg_1.withColumn('Tone', split_col.getItem(0))
gkg_3 = gkg_2.withColumn('Polarity', split_col.getItem(3))
gkg_4 = gkg_3.withColumn('Emotion_Charge', abs(gkg_3.Tone - gkg_3.Polarity))

# select Data from Events Dataset
events_1= events_df.select("GLOBALEVENTID", 'ActionGeo_CountryCode', "ActionGeo_Lat", "ActionGeo_Long", "NumMentions","NumSources","NumArticles","AvgTone")

## filter out events that have no geographic coordinates
#print('Total number of events: ', events_1.count())
events_2 = events_1.filter(events_1.ActionGeo_Lat.isNotNull())
#print('Number of events with geographic coordinates: ', events_2.count())

# merge the clean events and mentions dfs
event_mention = events_2.join(mentions_6, 'GLOBALEVENTID')

data_q1 = event_mention.join(gkg_4.select('DocumentIdentifier', 'Emotion_Charge'), gkg_4['DocumentIdentifier'] == event_mention['MentionIdentifier'])

#save the result
data_q1.write.mode("overwrite").parquet('/tmp/msadler/data_q1_new.parquet')
