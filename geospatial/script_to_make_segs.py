# Databricks notebook source
"""
This notebook will update the segmentations for Metro, Micro, NonMetro; State; Media Market; Census Region; and Census Division. 

It joins Preferred Store and Store DNA to create the segments. 

"""

# COMMAND ----------

# MAGIC %md
# MAGIC # job

# COMMAND ----------

#Define service principals

service_credential = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-pw')

service_application_id = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-app-id')

directory_id = "5f9dc6bd-f38a-454a-864c-c803691193c5"



# THIS CHANGES 
storage_account = 'sa8451pricepromoprd'

# COMMAND ----------

#Set configurations

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")

spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_application_id)

spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)

spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC # Set up

# COMMAND ----------

# common python packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from functools import reduce
from datetime import datetime, timedelta
import re

# spark packages
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *

# internal packages
from effodata import ACDS, golden_rules, Sifter, Equality, Joiner
import kayday as kd
from kpi_metrics import KPI, AliasMetric, CustomMetric, AliasGroupby, available_metrics, get_metrics
import seg
from seg.utils import DateType

# repo package
import geospatial.config as config

# COMMAND ----------

# MAGIC %md
# MAGIC # Preferred Store & Store DNA

# COMMAND ----------

# read in store DNA
SDNA = spark.read.format("delta").load("abfss://geospatial@sa8451geodev.dfs.core.windows.net/SDNA/GSC_SDNA")

# COMMAND ----------

# getting today's date to get most recent preferred store

today_date = datetime.now().strftime("%Y%m%d")

# COMMAND ----------

# read in preferred store

# https://seg.pages.8451.com/build/html/references/supported_files.html
# https://confluence.kroger.com/confluence/display/8451EC/Preferred+Store+Logic

preferred_store = seg.get_seg_for_date(seg="pref_store", date= today_date)

# COMMAND ----------

# read in states info -- this is for census region and division 

states_info = spark.read.option("header","true").csv("abfss://geospatial@sa8451geodev.dfs.core.windows.net/standard_geography/census_geography/census_state.csv")

# COMMAND ----------

# join preferred store to store dna 
# left join 

pref_store_and_dna = ((preferred_store
                      .join(SDNA, preferred_store['pref_store_code_1'] == SDNA['STORE_CODE'], how='left')
).join(states_info, SDNA['STATE'] == states_info['STUSPS'], how = 'left')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metro, Micro, NonMetro

# COMMAND ----------

# ehhn and metro

metro_seg_0 = pref_store_and_dna.select("ehhn", "CBSA_TYPE").na.drop()
metro_seg_1 = (metro_seg_0
               .withColumn("date", f.lit(today_date))
)

# COMMAND ----------

# requested by christain on 1/4/24 
# need to split up the metromicro df into 3 seperate dfs to allow easier access into prism
# will filter to particular segment
# will add a column for propensity called "H"

metropolitan = ((metro_seg_1
                .filter(f.col("CBSA_TYPE") == "Metropolitan Statistical Area")
                .withColumn("segment", f.lit("H"))
                )
                .drop("CBSA_TYPE")
)

micropolitan = ((metro_seg_1
                .filter(f.col("CBSA_TYPE") == "Micropolitan Statistical Area")
                .withColumn("segment", f.lit("H"))
                )
                .drop("CBSA_TYPE")
)

nonmetro = ((metro_seg_1
                .filter(f.col("CBSA_TYPE") == "NonMetro")
                .withColumn("segment", f.lit("H"))
                )
                .drop("CBSA_TYPE")
)

# COMMAND ----------

# QA CEHCK

# count 
orig_metro_count = metro_seg_1.filter(f.col("CBSA_TYPE") == "Metropolitan Statistical Area").count() 
orig_micro_count = metro_seg_1.filter(f.col("CBSA_TYPE") == "Micropolitan Statistical Area").count() 
orig_nonmetro_count = metro_seg_1.filter(f.col("CBSA_TYPE") == "NonMetro").count() 


print(f"the difference between the count of the original df metro_seg_1 and metropolitan is {orig_metro_count - metropolitan.count()}")
print(f"the difference between the count of the original df metro_seg_1 and micropolitan is {orig_micro_count - micropolitan.count()}")
print(f"the difference between the count of the original df metro_seg_1 and nonmetro is {orig_nonmetro_count - nonmetro.count()}")


# COMMAND ----------

# output paths 
output_path_metropolitan = f"{config.metro_micro_nonmetro_dir}/modality=metropolitan/date={today_date}"
output_path_micropolitan = f"{config.metro_micro_nonmetro_dir}/modality=micropolitan/date={today_date}"
output_path_nonmetro = f"{config.metro_micro_nonmetro_dir}/modality=nonmetro/date={today_date}"

# COMMAND ----------

metropolitan.write.mode("overwrite").format("delta").save(output_path_metropolitan)
micropolitan.write.mode("overwrite").format("delta").save(output_path_micropolitan)
nonmetro.write.mode("overwrite").format("delta").save(output_path_nonmetro)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## state

# COMMAND ----------

#ehhn and state 
state_seg = pref_store_and_dna.select("ehhn", "STATE").na.drop()
# state_seg.display()

# COMMAND ----------

# state_seg.write.mode("overwrite").format("delta").save(config.storage_state)

# COMMAND ----------

# MAGIC %md
# MAGIC ## media market

# COMMAND ----------

#ehhn and media market 
media_market_seg = pref_store_and_dna.select("ehhn", "IRI_MEDIA_MKT_NAME").na.drop()
# media_market_seg.display()

# COMMAND ----------

# media_market_seg.write.mode("overwrite").format("delta").save(config.storage_media_market)

# COMMAND ----------

# MAGIC %md
# MAGIC ## census region

# COMMAND ----------

cen_region_seg = pref_store_and_dna.select("ehhn", "CEN_REG").na.drop()

# COMMAND ----------

# cen_region_seg.write.mode("overwrite").format("delta").save(config.storage_census_region)

# COMMAND ----------

# MAGIC %md
# MAGIC ## census division

# COMMAND ----------

cen_division_seg = pref_store_and_dna.select("ehhn", "CEN_DIV").na.drop()

# COMMAND ----------

# cen_division_seg.write.mode("overwrite").format("delta").save(config.storage_census_division)
