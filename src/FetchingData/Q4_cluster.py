import pyspark
import os
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

# # # schema of the dataset
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

# read data by months (not all at once)
Years  = ["2015","2016","2017"]
Months = ["01","02","03","04","05","06","07","08","09","10","11","12"]

for y in Years:
    for m in Months:
        try:
        	gkg_df      = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, y+m+"*.gkg.csv"),schema=GKG_SCHEMA)
	        events_df   = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, y+m+"*.export.CSV"),schema=EVENTS_SCHEMA)
	        mentions_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, y+m+"*.mentions.CSV"),schema=MENTIONS_SCHEMA)
	        
	        ### get the countries from the url.csv
	        # read file
	        Domains = spark.read.format("csv").option("header", "true").load("urls.csv")
	        # rename columns
	        Domains = Domains.select(Domains['alpha-2'].alias('code') ,Domains['name'].alias('country_source'), 
	                                 'region', Domains['web'].alias('url'))
	        # select what we want from mentions
	        mentions_1 = mentions_df.select("GLOBALEVENTID", "MentionType", "MentionIdentifier",
	                                       "MentionSourceName").filter(mentions_df["MentionType"] == '1')
	        # join and filter the dataframes
	        mentions_2 = mentions_1.join(Domains, Domains['url'] == mentions_1['MentionSourceName'], "left_outer")
	        mentions_df_wURL = mentions_2.filter(mentions_2.country_source.isNotNull())

	        # clean the MentionIdentifier column (as we want countries if there is a nan we can discard it)
	        mentions_df_wURL = mentions_df_wURL.na.drop(subset='MentionIdentifier',how="any")

	        # join gkg with the mentions so we get a dataframe with all the columns that interest us
	        GKG_MENTIONS_EVENTS = mentions_df_wURL["GLOBALEVENTID","MentionIdentifier","country_source","region"]\
	                                        .join(gkg_df["V2Tone","GCAM","DocumentIdentifier"], 
	                                        gkg_df['DocumentIdentifier'] == mentions_df_wURL['MentionIdentifier'], 'left_outer')



	        ### divide V2TONE column into specific columns
	        split_col = pyspark.sql.functions.split(GKG_MENTIONS_EVENTS['V2Tone'], ',')
	        GKG_MENTIONS_EVENTS = GKG_MENTIONS_EVENTS.withColumn('Tone', split_col.getItem(0))
	        GKG_MENTIONS_EVENTS = GKG_MENTIONS_EVENTS.withColumn('Positive Score', split_col.getItem(1))
	        GKG_MENTIONS_EVENTS = GKG_MENTIONS_EVENTS.withColumn('Negative Score', split_col.getItem(2))
	        GKG_MENTIONS_EVENTS = GKG_MENTIONS_EVENTS.withColumn('Polarity', split_col.getItem(3))
	        GKG_MENTIONS_EVENTS = GKG_MENTIONS_EVENTS.withColumn('Activity Reference Density', split_col.getItem(4))
	        GKG_MENTIONS_EVENTS = GKG_MENTIONS_EVENTS.withColumn('Self/Group Reference Density', split_col.getItem(5))
	        GKG_MENTIONS_EVENTS = GKG_MENTIONS_EVENTS.withColumn('Word_Count', split_col.getItem(6))

	        ### get a dataframe with only the columns that we need and change types for future operations
	        Essential_Q4_df = GKG_MENTIONS_EVENTS.select("country_source","Tone","Polarity","Word_Count","GCAM")
	        Essential_Q4_df = Essential_Q4_df.withColumn("Tone", Essential_Q4_df["Tone"].cast(DoubleType()))
	        Essential_Q4_df = Essential_Q4_df.withColumn("Word_Count", Essential_Q4_df["Word_Count"].cast(DoubleType()))

	        ### transformations for our Emotion Charge metric
	        # get the absolute value of tone
	        Essential_Q4_df = Essential_Q4_df.withColumn("Tone",abs(Essential_Q4_df.Tone))

	        # get maximum and min value of word count to normalize data
	        max_v = Essential_Q4_df.agg({"Word_Count": "max"}).collect()[0][0]
	        min_v = Essential_Q4_df.agg({"Word_Count": "min"}).collect()[0][0]

	        # normalize word count column
	        Essential_Q4_df = Essential_Q4_df.withColumn('WordCount_Norm', (Essential_Q4_df.Word_Count-min_v)/(max_v-min_v))

	        # create new column that translates emotionality of that event
	        Essential_Q4_df = Essential_Q4_df.withColumn("Emotion_charge",abs(Essential_Q4_df.Tone - Essential_Q4_df.Polarity)\
	                                                     *Essential_Q4_df.WordCount_Norm)
	        # group by country
	        Final_df = Essential_Q4_df.groupby(Essential_Q4_df.country_source).agg(sum("Emotion_charge"),
	                                                                    count("country_source"))

	        # rename columns to be possible to save in parquet file
	        Final_df = Final_df.select( Final_df['country_source'].alias('CountrySource'),
	                                    Final_df['sum(Emotion_charge)'].alias('EmotionCharge') ,
	                                    Final_df['count(country_source)'].alias('CountCountrySource'))
            
	        # add a new column that takes into account how many events happened in each country
	        Final_df = Final_df.withColumn("NormEvnt",Final_df["EmotionCharge"] / Final_df["CountCountrySource"])

	        # save final dataframe as a parquet file
	        Final_df.write.mode("overwrite").parquet("/tmp/abranche/" +y+m+ "df.parquet")

        except:
            print("There are no files for this time: ", y+m)
        


