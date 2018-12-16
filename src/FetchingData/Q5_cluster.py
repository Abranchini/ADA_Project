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

Word_list = ['c1.1', 'c2.7', 'c2.8', 'c2.9', 'c2.10', 'c2.12', 'c2.13', 'c2.16',
       'c2.17', 'c2.18', 'c2.19', 'c2.22', 'c2.23', 'c2.24', 'c2.26',
       'c2.28', 'c2.29', 'c2.32', 'c2.35', 'c2.40', 'c2.41', 'c2.45',
       'c2.46', 'c2.48', 'c2.55', 'c2.56', 'c2.57', 'c2.59', 'c2.60',
       'c2.61', 'c2.63', 'c2.64', 'c2.65', 'c2.66', 'c2.67', 'c2.68',
       'c2.69', 'c2.70', 'c2.71', 'c2.72', 'c2.73', 'c2.74', 'c2.79',
       'c2.81', 'c2.82', 'c2.86', 'c2.87', 'c2.91', 'c2.93', 'c2.94',
       'c2.95', 'c2.96', 'c2.97', 'c2.99', 'c2.103', 'c2.104', 'c2.105',
       'c2.108', 'c2.109', 'c2.110', 'c2.113', 'c2.115', 'c2.117',
       'c2.119', 'c2.121', 'c2.122', 'c2.123', 'c2.124', 'c2.128',
       'c2.129', 'c2.147', 'c2.148', 'c2.149', 'c2.151', 'c2.152',
       'c2.156', 'c2.159', 'c2.160', 'c2.171', 'c2.172', 'c2.173',
       'c2.174', 'c2.175', 'c2.176', 'c2.177', 'c2.178', 'c2.179',
       'c2.186', 'c2.188', 'c2.190', 'c2.191', 'c2.192', 'c2.193',
       'c2.194', 'c2.197', 'c2.199', 'c2.202', 'c2.205', 'c2.206',
       'c2.207', 'c2.210', 'c2.211', 'c2.212', 'c2.213', 'c2.214',
       'c2.225', 'c2.226', 'c2.227', 'c2.228', 'c3.1', 'c3.2', 'c4.1',
       'c4.3', 'c4.4', 'c4.5', 'c4.6', 'c4.7', 'c4.8', 'c4.9', 'c4.10',
       'c4.11', 'c4.12', 'c4.13', 'c4.15', 'c4.16', 'c4.17', 'c4.23',
       'c4.25', 'c4.27', 'c4.28', 'c5.1', 'c5.2', 'c5.3', 'c5.4', 'c5.5',
       'c5.6', 'c5.7', 'c5.8', 'c5.9', 'c5.10', 'c5.11', 'c5.12', 'c5.13',
       'c5.14', 'c5.15', 'c5.16', 'c5.17', 'c5.18', 'c5.19', 'c5.20',
       'c5.21', 'c5.22', 'c5.23', 'c5.24', 'c5.25', 'c5.26', 'c5.28',
       'c5.29', 'c5.31', 'c5.32', 'c5.33', 'c5.36', 'c5.37', 'c5.38',
       'c5.39', 'c5.40', 'c5.41', 'c5.43', 'c5.44', 'c5.45', 'c5.46',
       'c5.47', 'c5.48', 'c5.49', 'c5.50', 'c5.52', 'c5.53', 'c5.55',
       'c5.57', 'c5.58', 'c5.59', 'c5.61', 'c6.1', 'c6.4', 'c6.5', 'c6.6',
       'c7.1', 'c7.2', 'c10.1', 'c10.2', 'c11.1', 'c14.1', 'c14.2',
       'c14.5', 'c14.6', 'c14.9', 'c14.11', 'c15.1', 'c15.2', 'c15.3',
       'c15.4', 'c15.5', 'c15.6', 'c15.7', 'c15.8', 'c15.12', 'c15.13',
       'c15.14', 'c15.15', 'c15.16', 'c15.17', 'c15.18', 'c15.19',
       'c15.20', 'c15.21', 'c15.22', 'c15.23', 'c15.24', 'c15.25',
       'c15.26', 'c15.27', 'c15.28', 'c15.29', 'c15.30', 'c15.32',
       'c15.33', 'c15.34', 'c15.35', 'c15.36', 'c15.38', 'c15.39',
       'c15.41', 'c15.42', 'c15.43', 'c15.44', 'c15.45', 'c15.46',
       'c15.47', 'c15.48', 'c15.50', 'c15.51', 'c15.52', 'c15.53',
       'c15.54', 'c15.55', 'c15.56', 'c15.57', 'c15.58', 'c15.59',
       'c15.60', 'c15.61', 'c15.62', 'c15.63', 'c15.64', 'c15.65',
       'c15.66', 'c15.67', 'c15.68', 'c15.69', 'c15.70', 'c15.71',
       'c15.72', 'c15.74', 'c15.75', 'c15.76', 'c15.77', 'c15.78',
       'c15.79', 'c15.80', 'c15.81', 'c15.82', 'c15.83', 'c15.84',
       'c15.85', 'c15.86', 'c15.87', 'c15.88', 'c15.89', 'c15.90',
       'c15.91', 'c15.92', 'c15.93', 'c15.94', 'c15.95', 'c15.96',
       'c15.97', 'c15.98', 'c15.99', 'c15.100', 'c15.101', 'c15.102',
       'c15.103', 'c15.105', 'c15.106', 'c15.107', 'c15.108', 'c15.109',
       'c15.110', 'c15.112', 'c15.113', 'c15.114', 'c15.115', 'c15.116',
       'c15.117', 'c15.118', 'c15.119', 'c15.120', 'c15.121', 'c15.122',
       'c15.123', 'c15.124', 'c15.126', 'c15.127', 'c15.128', 'c15.130',
       'c15.131', 'c15.132', 'c15.133', 'c15.134', 'c15.135', 'c15.136',
       'c15.137', 'c15.138', 'c15.139', 'c15.140', 'c15.141', 'c15.142',
       'c15.143', 'c15.144', 'c15.145', 'c15.146', 'c15.147', 'c15.148',
       'c15.149', 'c15.150', 'c15.151', 'c15.152', 'c15.153', 'c15.154',
       'c15.155', 'c15.156', 'c15.157', 'c15.158', 'c15.159', 'c15.160',
       'c15.161', 'c15.162', 'c15.163', 'c15.164', 'c15.165', 'c15.166',
       'c15.167', 'c15.169', 'c15.170', 'c15.171', 'c15.173', 'c15.174',
       'c15.176', 'c15.178', 'c15.179', 'c15.180', 'c15.181', 'c15.182',
       'c15.183', 'c15.184', 'c15.185', 'c15.186', 'c15.187', 'c15.188',
       'c15.189', 'c15.190', 'c15.191', 'c15.192', 'c15.193', 'c15.194',
       'c15.195', 'c15.196', 'c15.202', 'c15.203', 'c15.204', 'c15.205',
       'c15.206', 'c15.207', 'c15.208', 'c15.209', 'c15.210', 'c15.215',
       'c15.216', 'c15.217', 'c15.219', 'c15.221', 'c15.222', 'c15.223',
       'c15.224', 'c15.225', 'c15.226', 'c15.227', 'c15.228', 'c15.229',
       'c15.231', 'c15.232', 'c15.233', 'c15.234', 'c15.235', 'c15.236',
       'c15.237', 'c15.238', 'c15.239', 'c15.240', 'c15.241', 'c15.242',
       'c15.243', 'c15.244', 'c15.245', 'c15.247', 'c15.248', 'c15.250',
       'c15.251', 'c15.252', 'c15.253', 'c15.254', 'c15.255', 'c15.256',
       'c15.257', 'c15.258', 'c15.259', 'c15.260', 'c15.261', 'c15.262',
       'c15.263', 'c15.264', 'c15.265', 'c15.266', 'c15.267', 'c15.268',
       'c15.269', 'c15.270', 'c15.271', 'c15.272', 'c15.273', 'c15.274',
       'c15.275', 'c15.276', 'c15.277', 'c15.278', 'c15.279', 'c15.280',
       'c16.1', 'c16.2', 'c16.3', 'c16.4', 'c16.6', 'c16.7', 'c16.9',
       'c16.10', 'c16.11', 'c16.12', 'c16.13', 'c16.14', 'c16.15',
       'c16.16', 'c16.17', 'c16.19', 'c16.20', 'c16.21', 'c16.22',
       'c16.23', 'c16.24', 'c16.25', 'c16.26', 'c16.29', 'c16.30',
       'c16.31', 'c16.32', 'c16.33', 'c16.34', 'c16.35', 'c16.36',
       'c16.37', 'c16.39', 'c16.40', 'c16.41', 'c16.42', 'c16.43',
       'c16.44', 'c16.45', 'c16.46', 'c16.47', 'c16.48', 'c16.49',
       'c16.50', 'c16.51', 'c16.52', 'c16.53', 'c16.54', 'c16.55',
       'c16.56', 'c16.57', 'c16.58', 'c16.59', 'c16.60', 'c16.61',
       'c16.62', 'c16.63', 'c16.64', 'c16.66', 'c16.67', 'c16.68',
       'c16.69', 'c16.70', 'c16.71', 'c16.72', 'c16.73', 'c16.74',
       'c16.76', 'c16.77', 'c16.78', 'c16.79', 'c16.80', 'c16.81',
       'c16.82', 'c16.83', 'c16.84', 'c16.85', 'c16.86', 'c16.87',
       'c16.88', 'c16.89', 'c16.90', 'c16.91', 'c16.92', 'c16.93',
       'c16.94', 'c16.95', 'c16.96', 'c16.97', 'c16.98', 'c16.99',
       'c16.100', 'c16.101', 'c16.102', 'c16.103', 'c16.104', 'c16.105',
       'c16.106', 'c16.107', 'c16.108', 'c16.109', 'c16.110', 'c16.111',
       'c16.112', 'c16.113', 'c16.114', 'c16.115', 'c16.116', 'c16.117',
       'c16.118', 'c16.120', 'c16.121', 'c16.122', 'c16.123', 'c16.124',
       'c16.126', 'c16.127', 'c16.129', 'c16.130', 'c16.132', 'c16.133',
       'c16.134', 'c16.136', 'c16.138', 'c16.139', 'c16.140', 'c16.141',
       'c16.142', 'c16.143', 'c16.145', 'c16.146', 'c16.147', 'c16.148',
       'c16.149', 'c16.150', 'c16.152', 'c16.153', 'c16.154', 'c16.155',
       'c16.156', 'c16.157', 'c16.158', 'c16.160', 'c16.161', 'c16.163',
       'c16.164', 'c16.165', 'c16.166', 'c16.167', 'c16.168', 'c18.1',
       'c18.2', 'c18.3', 'c18.6', 'c18.7', 'c18.8', 'c18.9', 'c18.10',
       'c18.12', 'c18.16', 'c18.18', 'c18.19', 'c18.20', 'c18.21',
       'c18.22', 'c18.23', 'c18.25', 'c18.26', 'c18.27', 'c18.28',
       'c18.29', 'c18.32', 'c18.33', 'c18.44', 'c18.45', 'c18.48',
       'c18.49', 'c18.50', 'c18.52', 'c18.66', 'c18.67', 'c18.69',
       'c18.72', 'c18.75', 'c18.77', 'c18.78', 'c18.79', 'c18.80',
       'c18.81', 'c18.82', 'c18.83', 'c18.86', 'c18.88', 'c18.89',
       'c18.90', 'c18.92', 'c18.95', 'c18.96', 'c18.97', 'c18.98',
       'c18.99', 'c18.100', 'c18.101', 'c18.103', 'c18.104', 'c18.110',
       'c18.114', 'c18.115', 'c18.120', 'c18.122', 'c18.123', 'c18.125',
       'c18.131', 'c18.132', 'c18.133', 'c18.135', 'c18.136', 'c18.137',
       'c18.138', 'c18.139', 'c18.140', 'c18.141', 'c18.142', 'c18.143',
       'c18.144', 'c18.145', 'c18.147', 'c18.149', 'c18.151', 'c18.152',
       'c18.155', 'c18.161', 'c18.164', 'c18.165', 'c18.182', 'c18.183',
       'c18.184', 'c18.197', 'c18.198', 'c18.199', 'c18.200', 'c18.201',
       'c18.204', 'c18.205', 'c18.206', 'c18.207', 'c18.208', 'c21.1',
       'c26.1', 'c35.31', 'c35.32', 'c35.33', 'c39.1', 'c39.2', 'c39.3',
       'c39.7', 'c39.10', 'c39.11', 'c39.12', 'c39.13', 'c39.16',
       'c39.17', 'c39.18', 'c39.19', 'c39.20', 'c39.21', 'c39.22',
       'c39.23', 'c39.24', 'c39.27', 'c39.28', 'c39.29', 'c39.30',
       'c39.31', 'c39.32', 'c39.33', 'c39.34', 'c39.35', 'c39.39',
       'c39.40', 'c39.41', 'c40.1', 'c40.2', 'c40.3', 'c40.4', 'c40.5',
       'c40.6', 'c22.10', 'c22.11', 'c24.1', 'c24.2', 'c24.3', 'c24.4',
       'c24.5', 'c24.6', 'c24.7', 'c24.8', 'c24.9', 'c24.10', 'c24.11',
       'c27.1', 'c36.31', 'c36.32', 'c36.33', 'c37.31', 'c37.32',
       'c37.33', 'c41.1', 'c41.2', 'c41.3']


# cluster directory
DATA_DIR = 'hdfs:///datasets/gdeltv2'
DATA_LOCAL = ''

# define udf
differencer = udf(lambda x,y: list(set(y)-(set(y)-set(x))), ArrayType(StringType()))

# read data by months (not all at once)
Years  = ["2015","2016","2017"]
Months = ["01","02","03","04","05","06","07","08","09","10","11","12"]
Days   = ["0423","1209","2017"]

for y in Years:
    for m in Months:
    	for d in Days:
	        try:
	        	gkg_df      = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, y+m+d+"*.gkg.csv"),schema=GKG_SCHEMA)
		        events_df   = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, y+m+d+"*.export.CSV"),schema=EVENTS_SCHEMA)
		        mentions_df = spark.read.option("sep", "\t").csv(os.path.join(DATA_DIR, y+m+d+"*.mentions.CSV"),schema=MENTIONS_SCHEMA)
		        
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

		        GKG_EVENTS = mentions_df_wURL["GLOBALEVENTID","MentionIdentifier","country_source"].join(gkg_df["GCAM","DocumentIdentifier"],
		        										gkg_df["DocumentIdentifier"]==mentions_df_wURL["MentionIdentifier"], "left_outer")

		        Essential_Q4_df = GKG_EVENTS.select("country_source","GCAM")

		        Emot_Words_df = Essential_Q4_df.select("country_source","GCAM",
		        	split(col("GCAM"), ":").alias("GCAM2"))

		        Emot_Words_df = Emot_Words_df.withColumn("GCAM2", concat_ws(',', 'GCAM2'))

		        Emot_Words_df = Emot_Words_df.select("country_source",split(col("GCAM2"), ",").alias("GCAM"))

		        Emot_Words_df = Emot_Words_df.withColumn("HRE", array([lit(x) for x in Word_list]))

		        Emot_Words_df = Emot_Words_df.withColumn('DIF', differencer('HRE', 'GCAM'))

		        Final_df = Emot_Words_df.groupby(Emot_Words_df.country_source).agg(collect_set(col("DIF")).alias("SpeechWordsList"))

		        # save final dataframe as a parquet file
		        Final_df.write.mode("overwrite").parquet("/tmp/abranche/" +y+m+d+ "_Wdictionaries.parquet")


	        except:
	            print("There are no files for this time: ", y+m+d)
        


