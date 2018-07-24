# -*- coding: utf-8 -*-
#!/usr/bin/python

from __future__ import absolute_import
import json
import pprint
import subprocess
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col,sum,split
from pyspark.sql.types import StringType, IntegerType

import re
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import unicodedata
from datetime import datetime, timedelta


from pyspark.sql import Row 


sc = pyspark.SparkContext()
sqlContext = SQLContext(sc)

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the InputFormat. This assumes the Cloud Storage connector for
# Hadoop is configured.
bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
todays_date = datetime.strftime(datetime.today(), "%Y-%m-%d-%H-%M-%S")
input_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_input_{}'.format(bucket,todays_date)

################ Fonctions utiles #################
def remove_accents(input_str):
    try :
        nfkd_form = unicodedata.normalize('NFKD', input_str)
        only_ascii = nfkd_form.encode('ASCII', 'ignore')
        return only_ascii
    except :
        return ""

def extract_strat_data(placement_name):
    try :
        strat_data = re.search(r'Cible Comportemental (.*?)_', placement_name).group(1)
        return strat_data
    except :
        return ""

def stringify(input):
    return str(input)

def find_gaID(QueryString):
    try :
        text = re.search(r'GAClientID=(.*?)&', QueryString).group(1)
        return text
    except :
        return ""

###################################################


################# I. LOADING DATA #################


############# 1. Import GA table from Big Query 


#############

############# 2. Import Sizmek table from Google Cloud Storage 
# On lit de rapport global de Sizmek depuis GCS -> fichier qui contient les informations des placementName et stratData que l'on va pouvoir recuperer
# en recoupant avec l'adID.

udfUnicode = udf(remove_accents, StringType())
udfStratData = udf(extract_strat_data, StringType())
udfString = udf(stringify, StringType())
udfGAID = udf(find_gaID, StringType())


df_campaign_info = sqlContext.read.csv("gs://cloud-composer-bucket-testing/SmallReport.csv", header=True, sep = ",")
df_campaign_info = df_campaign_info.withColumn("Campaign Name", udfUnicode("Campaign Name")) # On retire les accents mal parse du nom de campagne
df_campaign_info = df_campaign_info.filter(df_campaign_info["Campaign Name"]  == "L-Fte des mres Programmatique PBU-03052018") # on filtre sur la campagne programmatique
df_campaign_info = df_campaign_info.withColumn("stratData", udfStratData("Placement Name")) # On extrait la stratData du placementName
df_campaign_info = df_campaign_info.withColumn("Ad ID", udfString("Ad ID")) # On stringify l'AD ID car ce dernier est en string dans le rapport granulaire
df_campaign_info = df_campaign_info.select(["Ad ID", "stratData", "Placement Name"]) # on garde seulement les colonnes adID, stratData et placementName

# On lit de rapport granulaire de Sizmek depuis GCS -> fichier qui contient les logs sizmek.
# On va s'interesser au QueryStrig nous donnant le gaID
# On va s'intereser aux dimensions du WinningEvent -> (dernier point de contact)
df_sizmek = sqlContext.read.csv("gs://cloud-composer-bucket-testing/fatImport1.csv", header=True, sep = ",")
df_sizmek = df_sizmek.select(["QueryString_0_0", "WinningEventType", "WinningEventDate", "WinningEventEntityID", "WinningCampaignName"]) # on garde seulement certaines colonnes
df_sizmek = df_sizmek.filter(df_sizmek.WinningCampaignName.isNotNull()) # on garde les lignes ou les campagnes sont non nulles
df_sizmek = df_sizmek.withColumn("WinningCampaignName", udfUnicode("WinningCampaignName")) # on retire les accents des campagnes
df_sizmek = df_sizmek.filter(df_sizmek.WinningCampaignName  == "L-Fte des mres Programmatique PBU-03052018") # on filtre sur la camapgne programmatique PBU
df_sizmek = df_sizmek.withColumn("gaID", udfGAID("QueryString_0_0")) # on extrait le gaID du QueryString
df_sizmek = df_sizmek.filter(df_sizmek.gaID != "n/a") # on garde seulement les lignes avec gaID bien defini -> lorsque non defini ils mettent n/a

df_enriched = df_sizmek.join(df_campaign_info, df_sizmek.WinningEventEntityID == df_campaign_info["Ad ID"], "left_outer")
#df_enriched.show(10)
#############

############# 3. Import data for SEA Ranking and impression 
from pyspark.sql.types import StringType, IntegerType, DateType
from datetime import datetime
stringtodate =  udf (lambda x: datetime.strptime(x, '%d/%m/%Y'), TimestampType())
df_sea = sqlContext.read.csv("gs://cloud-composer-bucket-testing/data_SEA_programmatic.csv", header=True, sep = ";")
df_sea = df_sea.select(stringtodate(col("Day")).alias("day_sea") ,
    col("Impressions").cast(FloatType()).alias("impressions_sea"),
    col("Avg_position").cast(FloatType()).alias("avg_position_sea"),
    col("Search_Exact_match_IS").cast(FloatType()).alias("EMIS_sea"), 
    col("Clicks").cast(FloatType()).alias("clicks_sea"))
#df_sea.show(10)
#############

############# 4.  Import data for social 
from pyspark.sql.types import StringType, IntegerType, DateType
from datetime import datetime
stringtodate =  udf (lambda x: datetime.strptime(x, '%d/%m/%Y'), TimestampType())
df_social = sqlContext.read.csv("gs://cloud-composer-bucket-testing/data_social.csv", header=True, sep = ";")

df_social = df_social.select(col("Reach").cast(FloatType()).alias("reach_social"), 
    col("Impressions").cast(FloatType()).alias("impressions_social"),
    col("Frequency").cast(FloatType()).alias("frequency_social"),
    col("Clics sur un lien").cast(FloatType()).alias("clicks_social"),
    stringtodate(col("Reporting Starts")).alias("reporting_start_social"),
    stringtodate(col("Reporting Ends")).alias("reporting_end_social"))
#df_social.show(10)
###############
###################################################


################## II. Data prep ##################

############### 1. GA 


################## II. Data prep ##################

######### 1. 

conf = {
    # Input Parameters.
    'mapred.bq.project.id': project,
    'mapred.bq.gcs.bucket': bucket,
    'mapred.bq.temp.gcs.path': input_directory,
    'mapred.bq.input.project.id': 'channel-cloud',
    'mapred.bq.input.dataset.id': 'Media_Dashboard_Test_DMP_Fr',
    'mapred.bq.input.table.id': '01_tableRaw_limit_NA',
}

# Load data in from BigQuery.
ga_clo = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=conf)



print(ga_clo.take(2)) # print + .take() pour voir contenu rdd

def transform(row):
    out_dict = {}
    # convert string to dict 
    row_list = eval(row[1])
    out_dict['date'] = row_list["date"]
    out_dict["visitStartTime"] = row_list["visitStartTime"]
    out_dict["time"] = row_list["time"]
    out_dict['fullVisitorId'] = row_list["fullVisitorId"]
    out_dict['visitId'] = row_list["visitId"]
    out_dict['concat_visitId_fullVisitor'] = row_list["visitId"]+'_'+row_list["fullVisitorId"]
    out_dict['category'] = row_list["category"]
    out_dict['division'] = row_list["division"]
    out_dict['locale'] = row_list["locale"]
    out_dict['cookie_ID_ga'] = row_list["cookie_ID_ga"]
    out_dict['channel'] = row_list["channel"]
    out_dict['campaign'] = row_list["campaign"]
    out_dict['campaign_id'] = row_list["campaign_id"]
    out_dict['event_cat'] = row_list["event_cat"]
    out_dict['event_act'] = row_list["event_act"]
    out_dict['event_lab'] = row_list["event_lab"]
    out_dict['bounce'] = row_list["bounce"]
    out_dict['sessions'] = row_list["sessions"]
    out_dict['pv'] = row_list["pv"]
    out_dict['transaction_id'] = row_list["transaction_id"]
    out_dict['transaction_item_id'] = row_list["transaction_item_id"]

    return Row(**out_dict)


mapping = ga_clo.map(transform)
df_ga = mapping.toDF()


# for coo in df_ga.columns :
#     print(coo)
#     df_ga[coo] = df_ga[coo].fillna("na", axis=0)

df_ga.printSchema()

##########################################

################## Jointure ##################
df_global = df_ga.join(df_sizmek, on = [df_sizmek.QueryString_0_0 == df_ga.cookie_ID_ga], how= 'full')
df_matche = df_ga.join(df_sizmek, on = [df_sizmek.QueryString_0_0 == df_ga.cookie_ID_ga], how= 'inner')
print("####### df_global")
df_global.show()
print(df_matche.count()) # TODO chequer le nombre de matcher 
df_global.groupBy("channel").agg({"channel" : 'count'}).show()
##########################################




