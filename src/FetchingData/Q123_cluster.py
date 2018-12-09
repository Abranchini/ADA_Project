import pyspark
import os
import scipy as sp
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp,desc, col, count, sum, asc, coalesce, udf, when, window, explode, unix_timestamp, lit
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


# cluster directory
DATA_DIR = 'hdfs:///datasets/gdeltv2'
DATA_LOCAL =

# open all the datasets
gkg_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.gkg.csv"),schema=GKG_SCHEMA)
events_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.export.CSV"),schema=EVENTS_SCHEMA)
mentions_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, "*.mentions.CSV"),schema=MENTIONS_SCHEMA)

#### open helper sets

# url to country dataset
Domains_txt = sc.textFile(DATA_LOCAL + 'GdeltDomainsByCountry-May18.txt')
Domains_rdd = Domains_txt.map(lambda x: x.split("\t"))
Domains = Domains_rdd.toDF(['url', 'code', 'country_source'])

############################### Question 1 ##############################################

# select the required data from Mentions Dataset
mentions_1 = mentions_df.select("GLOBALEVENTID", "EventTimeDate", "MentionType", "MentionSourceName") \
                .filter(mentions_df["MentionType"] == '1')

# join the domain df with the country corresponding to the source
mentions_2 = mentions_1.join(Domains, Domains['url'] == mentions_1['MentionSourceName'], "left_outer")

# filter out urls that are unknown
mentions_3 = mentions_2.filter(mentions_2.country_source.isNotNull())

# print the number of urls that have no country
print('number of unknown urls: ', mentions_2.filter("url is null").select('MentionSourceName').distinct().count())
print('number of known urls: ', mentions_3.select('url').distinct().count())

# join the file with the countries and capitals
mentions_4 = mentions_3.join(CountryToCapital, CountryToCapital['CountryName'] == mentions_3['country_source'], "left_outer")
print('Total number of articles with a country assigned: ', mentions_4.count())
print('Total number of articles with no country assigned: ', mentions_4.filter("CapitalLatitude is null").count())

# filter out rows with no geographic coordinates
mentions_5 = mentions_4.filter(mentions_4.CapitalLatitude.isNotNull())

# select relevant columns
mentions_6 = mentions_5.select('GLOBALEVENTID', 'EventTimeDate','CountryCode', 'CountryName', mentions_5['CapitalLatitude'].alias('Source_Lat'),
                                                   mentions_5['CapitalLongitude'].alias('Source_Long'))
mentions_6 = mentions_6.withColumn("Source_Lat", mentions_6["Source_Lat"].cast("float"))
mentions_6 = mentions_6.withColumn("Source_Long", mentions_6["Source_Long"].cast("float"))

# select Data from Events Dataset
events_1= events_df.select("GLOBALEVENTID", 'ActionGeo_CountryCode',"ActionGeo_Lat", "ActionGeo_Long", "NumMentions","NumSources","NumArticles","AvgTone")

# filter out events that have no geographic coordinates
print('Total number of events: ', events_1.count())
events_2 = events_1.filter(events_1.ActionGeo_Lat.isNotNull())
print('Number of events with geographic coordinates: ', events_2.count())

# merge the clean events and mentions dfs
data_q1 = events_2.join(mentions_6, 'GLOBALEVENTID')

# save the result

data_q1.write.parquet(DATA_LOCAL + 'data_q1.parquet')

############################### Question 2 ##############################################

data_q2 = events_2.groupBy('ActionGeo_CountryCode').agg(sum('NumArticles').alias('sum_articles'), count('GLOBALEVENTID').alias('count_events'))
data_q2.write.parquet(DATA_LOCAL + 'data_q2.parquet')

############################### Question 3 ##############################################

# dataset with the human development index
HDI_df = spark.read.format("csv").option("header", "true").load(DATA_LOCAL + "HDI_categories.csv")

#### create dataframe with country and country code from the domains dataframe
countrytocode = Domains.drop_duplicates(subset = ['code'])
# join country code
HDI_CountryCode = HDI_df.join(countrytocode.select('code', 'country_source'), countrytocode['country_source'] == HDI_df['Country'])

##### prepare the gkg file with the information needed

# filter on events that have count information
gkg_1 = gkg_df.filter(gkg_df.Counts.isNotNull())
CountType = pyspark.sql.functions.split(gkg_1['Counts'], '#')

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
events_1= events_df.select("GLOBALEVENTID", 'Actor1Religion1Code', 'Actor2Religion1Code',
                               'NumMentions',"AvgTone", 'GoldsteinScale', 'ActionGeo_CountryCode')

# filter events with no country code
events_2 = events_1.filter(events_1.ActionGeo_CountryCode.isNotNull())

# create religion column (take one of the two actors, because in the example test there was never data for both at the same time)
events_3 = events_2.withColumn('ActorReligion', coalesce(events_2['Actor1Religion1Code'], events_2['Actor2Religion1Code']))

# filter out columns with no religion
events_4 = events_3.filter(events_3.ActorReligion.isNotNull())

#### join the files togther

# join the HDI file to country code
events_5 = events_4.join(HDI_CountryCode.select('code', 'HDI'), HDI_CountryCode['code']==events_4['ActionGeo_CountryCode'])

# join event and mention file
mention_event = events_5.join(mentions_3, 'GLOBALEVENTID')

# join mention_event and gkg
gkg_mention_event = mention_event.join(gkg_3, mention_event['MentionIdentifier'] == gkg_3['DocumentIdentifier'])

data_q3 = gkg_mention_event.select(gkg_mention_event['ActionGeo_CountryCode'].alias('CountryEvent'), 'EventType',
                                   'ActorReligion', 'HDI', 'AvgTone',
                                  gkg_mention_event['country_source'].alias('CountrySource'))
data_q3 = data_q3.dropDuplicates()

##### generate and save feature counts

# ActorRelition
data_q3_ActorReligion = data_q3.groupBy("ActorReligion").count()
# EventType
data_q3_EventType = data_q3.groupBy("EventType").count()
# HDI
data_q3_HDI = data_q3.groupBy("HDI").count()
# CountryEvent
data_q3_CountryEvent = data_q3.groupBy("CountryEvent").count()
# CountrySource
data_q3_CountrySource = data_q3.groupBy("CountrySource").count()

# save everything
data_q3.write.parquet(DATA_LOCAL + 'data_q3.parquet')
data_q3_ActorReligion.write.parquet(DATA_LOCAL + 'data_q3_ActorReligion.parquet')
data_q3_EventType.write.parquet(DATA_LOCAL + 'data_q3_EventType.parquet')
data_q3_HDI.write.parquet(DATA_LOCAL + 'data_q3_HDI.parquet')
data_q3_CountryEvent.write.parquet(DATA_LOCAL + 'data_q3_CountryEvent.parquet')
data_q3_CountrySource.write.parquet(DATA_LOCAL + 'data_q3_CountrySource.parquet')
