import pyspark
import os
import scipy as sp
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp,desc, col, split, count, array, concat_ws, sum, min, max, asc, coalesce, udf, when, window, explode, unix_timestamp, lit
from pyspark.sql.types import FloatType

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

DATA_DIR = 'hdfs:///datasets/gdeltv2'

# open GDELT data
gkg_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.gkg.csv"),schema=GKG_SCHEMA)
events_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.export.CSV"),schema=EVENTS_SCHEMA)
mentions_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.mentions.CSV"),schema=MENTIONS_SCHEMA)

Domains = spark.read.format("csv").option("header", "true").load("urls.csv")
Domains = Domains.select(Domains['alpha-2'].alias('code') ,Domains['name'].alias('country_source'), 'region', Domains['web'].alias('url'))

# dataset with the human development index
HDI_df = spark.read.format("csv").option("header", "true").load("HDI_code_df.csv")

#### create dataframe with country and country code from the domains dataframe
#countrytocode = Domains.drop_duplicates(subset = ['code'])
# join country code
#HDI_CountryCode = HDI_df.join(countrytocode.select('code', 'country_source'), countrytocode['country_source'] == HDI_df['Country'])

##### prepare the gkg file with the information needed

# filter on events that have count information
gkg_1 = gkg_df.filter(gkg_df.Counts.isNotNull())
CountType = split(gkg_1['Counts'], '#')

# add the event type
gkg_2 = gkg_1.withColumn('EventType', CountType.getItem(0))

# add the number of people concerned
gkg_3 = gkg_2.withColumn('NumPeople', CountType.getItem(1))

###### prepare mention file
mentions_1 = mentions_df.select("GLOBALEVENTID", "MentionType", "MentionSourceName", 'MentionIdentifier') \
                        .filter(mentions_df["MentionType"] == '1')

# join the dataframe url to country
mentions_2 = mentions_1.join(Domains, Domains['url'] == mentions_1['MentionSourceName'], "left_outer")

# filter out urls that are unknown
mentions_3 = mentions_2.filter(mentions_2.country_source.isNotNull())

##### prepare event file
events_1= events_df.select("GLOBALEVENTID", 'Day_DATE','Actor1Religion1Code', 'Actor2Religion1Code',
                               'NumMentions',"AvgTone", 'GoldsteinScale', 'ActionGeo_CountryCode')

# filter events with no country code
events_2 = events_1.filter(events_1.ActionGeo_CountryCode.isNotNull())

# create religion column (take one of the two actors, because in the example test there was never data for both at the same time)
events_3 = events_2.withColumn('ActorReligion', coalesce(events_2['Actor1Religion1Code'], events_2['Actor2Religion1Code']))

# filter out columns with no religion
events_4 = events_3.filter(events_3.ActorReligion.isNotNull())

#### join the files togther

# join the HDI file to country code
events_5 = events_4.join(HDI_df.select('Country', 'HDI', 'FIPS_GDELT'), HDI_df['FIPS_GDELT']==events_4['ActionGeo_CountryCode'])

# join event and mention file
mention_event = events_5.join(mentions_3, 'GLOBALEVENTID')

# join mention_event and gkg
gkg_mention_event = mention_event.join(gkg_3, mention_event['MentionIdentifier'] == gkg_3['DocumentIdentifier'])


HRE = ['c2.152', 'c2.214', 'c3.2', 'c5.7', 'c6.5', 'c7.2', 'c10.1',
       'c14.9', 'c15.3', 'c15.4', 'c15.12', 'c15.26', 'c15.27', 'c15.30',
       'c15.36', 'c15.42', 'c15.53', 'c15.57', 'c15.61', 'c15.92',
       'c15.93', 'c15.94', 'c15.97', 'c15.101', 'c15.102', 'c15.103',
       'c15.105', 'c15.106', 'c15.107', 'c15.108', 'c15.109', 'c15.110',
       'c15.116', 'c15.120', 'c15.123', 'c15.126', 'c15.131', 'c15.136',
       'c15.137', 'c15.152', 'c15.171', 'c15.173', 'c15.179', 'c15.203',
       'c15.219', 'c15.221', 'c15.239', 'c15.260', 'c21.1', 'c35.31',
       'c24.1', 'c24.2', 'c24.3', 'c24.4', 'c24.5', 'c24.6', 'c24.7',
       'c24.8', 'c24.9', 'c24.10', 'c24.11', 'c36.31', 'c37.31', 'c41.1']

Emot_Words_df = gkg_mention_event.select(gkg_mention_event['ActionGeo_CountryCode'].alias('CountryEvent'), 'EventType',
                                   'ActorReligion', 'HDI', 'AvgTone', 'Day_DATE',
                                  'country_source', 'GCAM',split(col("GCAM"), ":").alias("GCAM2"))

Emot_Words_df = Emot_Words_df.withColumn('GCAM2', concat_ws(',', 'GCAM2'))

Emot_Words_df = Emot_Words_df.select('CountryEvent', 'EventType','ActorReligion', 'country_source', 'Day_DATE',
                                     'HDI', 'AvgTone', split(col("GCAM2"), ",").alias("GCAM"))

Emot_Words_df = Emot_Words_df.withColumn("HRE", array([lit(x) for x in HRE]) )

differencer = udf(lambda x,y: list(set(y)-(set(y)-set(x))), ArrayType(StringType()))
Emot_Words_df = Emot_Words_df.withColumn('DIF', differencer('HRE', 'GCAM'))

Emot_Words_df = Emot_Words_df.select('CountryEvent', 'EventType','ActorReligion', 'country_source', 'Day_DATE',
                                     'HDI', 'AvgTone', 'DIF')

data_q3 = Emot_Words_df.dropDuplicates()

##### generate and save feature counts

# ActorRelition
data_q3_ActorReligion = data_q3.groupBy("ActorReligion").count()
# EventType
data_q3_EventType = data_q3.groupBy("EventType").count()
# HDI
data_q3_HDI = data_q3.groupBy("HDI").count()
# CountryEvent
#data_q3_CountryEvent = data_q3.groupBy("CountryEvent").count()
# CountrySource
#data_q3_CountrySource = data_q3.groupBy("CountrySource").count()

# save everything

data_q3.write.mode("overwrite").parquet('/tmp/msadler/data_q3.parquet')
data_q3_ActorReligion.write.mode("overwrite").json('/tmp/msadler/data_q3_ActorReligion.json')
data_q3_EventType.write.mode("overwrite").json('/tmp/msadler/data_q3_EventType.json')
data_q3_HDI.write.mode("overwrite").json('/tmp/msadler/data_q3_HDI.json')
#data_q3_CountryEvent.write.mode("overwrite").parquet('/tmp/msadler/data_q3_CountryEvent.parquet')
#data_q3_CountrySource.write.mode("overwrite").parquet('/tmp/msadler/data_q3_CountrySource.parquet')
