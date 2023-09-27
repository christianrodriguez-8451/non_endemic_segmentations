# Databricks notebook source
"""
TO DO: Need to make a job for this! What do we want to do with Null values? Should I remove all? 


This notebook will update the segmentations for Metro, Micro, NonMetro; State; Media Market; Census Region; and Census Division. 

It joins Preferred Store and Store DNA to create the segments. 

"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Set up

# COMMAND ----------

# Databricks notebook source
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

metro_seg = pref_store_and_dna.select("ehhn", "CBSA_TYPE")
# metro_seg.display()

# COMMAND ----------

metro_seg.write.mode("overwrite").format("delta").save(config.storage_micro_metro_nonmetro)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## state

# COMMAND ----------

#ehhn and state 
state_seg = pref_store_and_dna.select("ehhn", "STATE")
# state_seg.display()

# COMMAND ----------

state_seg.write.mode("overwrite").format("delta").save(config.storage_state)

# COMMAND ----------

# MAGIC %md
# MAGIC ## media market

# COMMAND ----------

#ehhn and media market 
media_market_seg = pref_store_and_dna.select("ehhn", "IRI_MEDIA_MKT_NAME")
# media_market_seg.display()

# COMMAND ----------

media_market_seg.write.mode("overwrite").format("delta").save(config.storage_media_market)

# COMMAND ----------

# MAGIC %md
# MAGIC ## census region

# COMMAND ----------

cen_region_seg = pref_store_and_dna.select("ehhn", "CEN_REG")

# COMMAND ----------

cen_region_seg.write.mode("overwrite").format("delta").save(config.storage_census_region)

# COMMAND ----------

# MAGIC %md
# MAGIC ## census division

# COMMAND ----------

cen_division_seg = pref_store_and_dna.select("ehhn", "CEN_DIV")

# COMMAND ----------

cen_division_seg.write.mode("overwrite").format("delta").save(config.storage_census_division)

# COMMAND ----------


