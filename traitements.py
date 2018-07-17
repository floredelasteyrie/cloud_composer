#!/usr/bin/python

from __future__ import absolute_import
import json
import pprint
import subprocess
import pyspark
from pyspark.sql import SQLContext
from pyspark import sql

from pyspark.sql.types import Row


import re
from pyspark.sql import functions as sf 
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import unicodedata
from datetime import datetime, timedelta

sc = pyspark.SparkContext()
sqlContext = SQLContext(sc)

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the InputFormat. This assumes the Cloud Storage connector for
# Hadoop is configured.
bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
todays_date = datetime.strftime(datetime.today(), "%Y-%m-%d-%H-%M-%S")
input_directory = 'gs://{}/hadoop/tmp/bigquery/pyspark_input_{}'.format(bucket,todays_date)


################# LOADING DATA #################
################# Give the coordinates of BQ Table #########
################# project.id : channel-cloud ###### dataset.id : Media_Dashboard_Test_DMP_Fr
###### Table de test conteant un seul GA ID qui match avec Sizmek : test_1777079149
###### Table creee sur la periode de la campagne : table_GA_FDM_withPagePath

conf = {
    # Input Parameters.
    'mapred.bq.project.id': project,
    'mapred.bq.gcs.bucket': bucket,
    'mapred.bq.temp.gcs.path': input_directory,
    'mapred.bq.input.project.id': 'channel-cloud',
    'mapred.bq.input.dataset.id': 'Media_Dashboard_Test_DMP_Fr',
    'mapred.bq.input.table.id': 'GA',
}

# Output Parameters.
output_dataset = 'Media_Dashboard_Test_DMP_Fr'
output_table = 'table_raw_for_kpi_calcul'

############## I. Read / import data ##############
##### I.A Load GA data in from BigQuery : 
df_ga = sc.newAPIHadoopRDD('com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=conf)

#### I.A.1 Treat rdd format ####
# get the schema of the rdd 
# def f(x):
#     d = {}
#     for i in range(len(x)):
#         d[str(i)] = x[i]
#     return d
# df_ga = df_ga.map(lambda x: Row(**f(x))).toDF()
df_ga = df_ga.toDF()
df_ga.select('_2').show(truncate=False)
# schema = StructType([StructField(str(i), StringType(), True) for i in range(32)])

# df = sqlContext.createDataFrame(rdd, schema)


# Rename columns name
# for coo in df_ga.columns : 
#     df_ga.coo = 

###############################

print(type(df_ga))
df_ga.show(10) # Display 10 results.

##### I.B Load : 
df_sizmek = sqlContext.read.csv("gs://cloud-composer-bucket-testing/fatImport1.csv", header="true", sep = ",")

###################################################

############## II. Data preparation  ##############
##### II.A concat the two columns on GA table  :
df_ga = df_ga.withColumn('id_unique', sf.concat(sf.col('fullVisitorId'),sf.lit('_'), sf.col('visitId')))
df_ga.select(['fullVisitorId', 'visitId', 'id_unique']).show(5)

##### II.B select first 80 columns on sizmek table :
df_sizmek = df_sizmek.select([coo for coo in df_sizmek.columns if coo in df_sizmek.columns[:100]])
print(df_sizmek.dtypes)
print(len(df_sizmek.columns))
print(type(df_sizmek))
df_sizmek.show(5)

##### II.C Join sizmek and GA : 
df_global = df_ga.join(df_sizmek, on = [df_sizmek.QueryString_0_0 == df_ga.ClientID], how = 'inner') #TODO check the how 
nb_lignes_ga = df_ga.count()
nb_lignes_sizmek = df_sizmek.count()
nb_lignes_global = df_global.count()
print("Nb lignes ga : {} ; nb lignes Sizmek {} ; nb lignes global : {}".format(nb_lignes_ga, nb_lignes_sizmek, nb_lignes_global))
df_global.show(5)
################# FUNCTIONS #################





################# CAMPAIGN & SIZMEK ##################
#df_campaign_info = sqlContext.read.csv("gs://bucket-artefact-test/01_FDM/Delevery_Report_Chanel_300258_csv.csv", header="true", sep = ";")


# Manually clean up the staging_directories, otherwise BigQuery
# files will remain indefinitely.
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)
#output_path = sc._jvm.org.apache.hadoop.fs.Path(output_directory)
#output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(output_path, True)