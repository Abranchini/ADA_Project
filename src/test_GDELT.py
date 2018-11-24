import numpy as np
import pyspark
import os
import scipy as sp
from pyspark.sql import *
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *

%matplotlib inline

spark = SparkSession.builder.getOrCreate()

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
    StructField("NumMentions",LongType(),True),
    StructField("NumSources",LongType(),True),
    StructField("NumArticles",LongType(),True),
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


    ## Change this in the cluster

DATA_DIR = 'hdfs:///datasets/gdeltv2'

events_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.export.CSV"),schema=EVENTS_SCHEMA)
mentions_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.mentions.CSV"),schema=MENTIONS_SCHEMA)

# Question 1 Select Data from Mentions Dataset
mentions_q1_df = mentions_df.select("GLOBALEVENTID", "EventTimeDate", "MentionType", "MentionSourceName") \
                .filter(mentions_df["MentionType"] == '1')
# Question 1 Select Data from Events Dataset
events_q1_df= events_df.select("GLOBALEVENTID", "ActionGeo_Lat", "ActionGeo_Long", "NumMentions","NumSources","NumArticles","AvgTone")
# merge them
event_mentions_df = events_q1_df.join(mentions_q1_df, 'GLOBALEVENTID')
event_mentions_df.show(2,False)
