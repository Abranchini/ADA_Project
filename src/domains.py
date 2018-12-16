# coding: utf-8
import pandas as pd
import numpy as np
import pyspark
import os
import folium
import branca.colormap as cm

from pyspark.sql import *
from pyspark.sql.functions import to_timestamp, when
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


folder = 'C:\\Users\\Matyas\\Documents\\FEL\\Lausanne\\ada\\proj\\ADA_Project\\data\\'
folder_out = 'C:\\Users\\Matyas\\Documents\\FEL\\Lausanne\\ada\\proj\\ADA_Project\\output\\'

txt_file = sc.textFile(folder + 'UrlToCountry\\GdeltDomainsByCountry-May18.txt')


def plot_map(df, col, save=False, fileName=None):
    world_map = folder + 'Visualizations\\countries-land-10km.geo.json'

    color_scale = cm.linear.YlOrRd_04.to_step(40).scale(min(df[col]), max(df[col]))

    def get_color(code):
        country = df[df['alpha-3'] == code]['name'].count()
        if country == 0:
            # print(code)
            return '#8c8c8c'  # MISSING -> gray
        else:
            return color_scale(int(df[df['alpha-3'] == code][col]))

    m = folium.Map(location=[46.8, 8.3], tiles='cartodbpositron', zoom_start=2)
    folium.GeoJson(
        data=world_map,
        style_function=lambda feature: {
            'fillColor': get_color(feature['properties']['A3']),
            'fillOpacity': 0.7,
            'color': 'black',
            'weight': 1,
        }
    ).add_to(m)
    m.add_child(color_scale)

    if save and fileName is not None:
        m.save(outfile=folder_out + fileName + '.html')

    return m


rdd = txt_file.map(lambda x: x.split("\t"))
url_sources = rdd.toDF(['web', 'code', 'country'])
url_sources_pd = url_sources.toPandas()


# so far for the first 100 top countries
wrong_codes = ['GM','GA','GB','UK','RS','AU','AS','CN','CH','SV','ES','SP','TU','KN','SC','SE','SW','ZA','SF','UP','JA','SZ','IS','EZ','EI','VM','PO','CI','KR','KS','RP','DA','NU','NE','NG','NI','nic','SN','SG','sig','LO','IC','TK','TL','BM','BD','BG','BU','EN','LI','LS','LT','LH','BY','BO','RI','LG','MC','MN','MG','MA','MO','moc','CE','MU','PA','PM','LE','AJ','ZI','CJ','IZ','BH','BA','BK','BL','AG','YM','DO','DR','TO','TN','TS','GG','HA','WA','CB','BF','SU','KU','GJ','MP','GV','IV','ST','TP','TT','TD','NS','AV','AC','HO','CD','BC','TI','UV','CG','CF','RQ','AN','BN','MJ','KV','MI','FP','VI','VT','BX','MH','GQ','EK','PP','OD','WZ','VQ','GK','NH','SX','CK','TX','KT','PS','CT','AQ','AA','BP','CW','PC','MB','AY','CQ','PU','MF','RM','NT','FS','WE','FG']
right_codes = ['DE','GM','GA','GB','RU','AT','AU','KM','CN','SJ','SV','ES','TR','KP','KN','SC','SE','ZM','ZA','UA','JP','CH','IL','CZ','IE','VN','PT','CL','KI','KR','PH','DK','nic','NU','NE','NG','NI','sig','SN','SG','SK','IS','TC','TK','MM','BM','BD','BG','EE','LR','LI','LS','LT','BI','BY','RS','LV','moc','MC','MN','MG','MA','MO','LK','OM','PY','PA','LB','AZ','ZW','KY','IQ','BZ','BH','BA','BO','DZ','YE','DM','DO','TG','TO','TN','GE','HT','NA','KH','BS','SD','KW','GD','MU','GN','CI','LC','ST','TP','TT','SR','AI','AG','HN','TD','BW','TJ','BF','CD','CG','PR','AD','BJ','ME','XK','MW','PF','VG','VA','BN','MS','GU','GQ','PG','SS','SZ','VI','GG','VU','GS','CC','TM','CX','PW','CF','AS','AW','SB','CK','PN','MQ','AQ','MP','GW','YT','MH','AN','FQ','PS','GF']


corr_codes = pd.DataFrame({'wrong': wrong_codes, 'right': right_codes}, columns=['wrong', 'right'])

invalid_codes = ['CV','RB','OS','TT']
for code in invalid_codes:
    url_sources_pd = url_sources_pd[url_sources_pd['code'] != code]
    
url_sources_corr = pd.merge(url_sources_pd, corr_codes, how='left', left_on='code', right_on='wrong')
url_sources_corr.loc[url_sources_corr['right'].isnull(), 'right'] = url_sources_corr['code']
url_sources_corr.head()


url_sources_corr = url_sources_corr.drop(['code', 'wrong'], axis=1)
url_sources_corr = url_sources_corr.rename(index=str, columns={'right': 'code'})
url_sources_corr.head()


url_sum = url_sources_corr.groupby(['country', 'code'], as_index=False).count()
url_sum = url_sum.sort_values('web', ascending=False)
url_sum.head()


country_info = pd.read_csv(folder + 'CountryCodes\\countries-info.csv')
country_info = country_info[['name', 'alpha-2', 'alpha-3', 'region', 'sub-region']]
country_info.head(5)


df_urls = pd.merge(country_info, url_sources_corr, left_on='alpha-2', right_on='code')
df_urls = df_urls.drop(['country', 'code'], axis=1)
#df_urls.to_csv('./urls.csv')
df_urls.head()


df_urls_sum = df_urls.groupby(['name', 'alpha-2', 'alpha-3', 'region', 'sub-region'], as_index=False).count()
df_urls_sum = df_urls_sum.sort_values('web', ascending=False)
df_urls_sum['web-log'] = np.log(df_urls_sum['web'])
df_urls_sum.head()


events = sqlContext.read.parquet(folder + 'Results\\CountryEvent_number.parquet').toPandas()
reports = sqlContext.read.parquet(folder + 'Results\\CountrySource_number.parquet').toPandas()
codeconv_fips = pd.read_csv(folder + 'CountryCodes\\CountryCode_FIPS_alpha.csv')
codeconv_info = pd.read_csv(folder + 'CountryCodes\\countries-info.csv')
codeconv_info = codeconv_info[['name', 'alpha-2', 'alpha-3', 'region', 'sub-region']]


events_a2 = pd.merge(events, codeconv_fips, left_on='ActionGeo_CountryCode', right_on='FIPS_GDELT')
events_a3 = pd.merge(events_a2, codeconv_info, left_on='alpha-2', right_on='alpha-2')
events_a3 = events_a3[['name', 'alpha-2', 'alpha-3', 'count']]
events_a3['count-log'] = np.log(events_a3['count'])
events_a3['count-norm'] = (events_a3['count-log'] - events_a3['count-log'].min())/(events_a3['count-log'].max() - events_a3['count-log'].min())


reports_a3 = pd.merge(reports, codeconv_info, left_on='country_source', right_on='name')
reports_a3 = reports_a3[['name', 'alpha-2', 'alpha-3', 'count']]
reports_a3['count-log'] = np.log(reports_a3['count'])
reports_a3['count-norm'] = (reports_a3['count-log'] - reports_a3['count-log'].min())/(reports_a3['count-log'].max() - reports_a3['count-log'].min())
reports_a3.head()


plot_map(df_urls_sum, 'web-log', True, 'urls-log')

plot_map(reports_a3, 'count-log', True, 'reports-log')

plot_map(events_a3, 'count-log', True, 'events-log')