# Databricks notebook source
"""
Creates segmentations for Metro, Micro, and NonMetro.
Relies on Preferred Store and Store DNA to create the segments. Pulls
every household's preferred store from Preferred Store Database and then
gets every store's CBSA type (non-metro, micro, metro) from Store DNA.
"""

# COMMAND ----------

#Define service principals
service_credential = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-pw')
service_application_id = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-app-id')
directory_id = "5f9dc6bd-f38a-454a-864c-c803691193c5"
storage_account = 'sa8451pricepromoprd'

#Set configurations
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

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

#Read in Store DNA to get every store's cbsa type
store_dna = spark.read.format("delta").load("abfss://geospatial@sa8451geodev.dfs.core.windows.net/SDNA/GSC_SDNA")

#Read in Preferred Store to get every household's preferred store
# https://seg.pages.8451.com/build/html/references/supported_files.html
# https://confluence.kroger.com/confluence/display/8451EC/Preferred+Store+Logic
today_date = datetime.now().strftime("%Y%m%d")
preferred_store = seg.get_seg_for_date(seg="pref_store", date=today_date)

# COMMAND ----------

#Merge preferred store w/ store dna to get every preferred store their CBSA type
preferred_store = preferred_store.withColumnRenamed('pref_store_code_1', 'store_code')
store_dna = store_dna.withColumnRenamed('STORE_CODE', 'store_code')
preferred_store = preferred_store.join(store_dna,
                                       "store_code",
                                       how='left')
ehhn_cbsa = preferred_store.select("ehhn", "CBSA_TYPE").na.drop()

#Separate the households by cbsa type and assignment H to each household
#Makes the deployment and audience template creation simple
ehhn_cbsa = ehhn_cbsa.withColumn("segment", f.lit("H"))
metropolitan = ehhn_cbsa.filter(f.col("CBSA_TYPE") == "Metropolitan Statistical Area")
micropolitan = ehhn_cbsa.filter(f.col("CBSA_TYPE") == "Micropolitan Statistical Area")
nonmetro = ehhn_cbsa.filter(f.col("CBSA_TYPE") == "NonMetro")

metropolitan = metropolitan.drop("CBSA_TYPE")
micropolitan = micropolitan.drop("CBSA_TYPE")
nonmetro = nonmetro.drop("CBSA_TYPE")

# COMMAND ----------

#QC Check by getting household count per cbsa type
ehhn_cbsa = ehhn_cbsa.groupBy('CBSA_TYPE').agg(f.count('CBSA_TYPE').alias('household_count'))
ehhn_cbsa.show(10, truncate=False)

# COMMAND ----------

#Write out the segmentation files
metro_fp = f"{config.metro_micro_nonmetro_dir}metropolitan/metropolitan_{today_date}"
micro_fp = f"{config.metro_micro_nonmetro_dir}micropolitan/micropolitan_{today_date}"
nonmetro_fp = f"{config.metro_micro_nonmetro_dir}nonmetro/nonmetro_{today_date}"

metropolitan.write.mode("overwrite").format("delta").save(metro_fp)
micropolitan.write.mode("overwrite").format("delta").save(micro_fp)
nonmetro.write.mode("overwrite").format("delta").save(nonmetro_fp)
