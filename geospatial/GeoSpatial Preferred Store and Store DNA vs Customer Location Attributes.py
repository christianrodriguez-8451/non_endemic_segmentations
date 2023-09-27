# Databricks notebook source
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

def groupby_count_perc(df, col_to_groupby, col_to_count, prefix):

  grouped_df = (df
                .groupBy(col_to_groupby)
                .agg(f.count(col_to_count).alias(f"{prefix}_hh_count"))
  )

  # Calculate the total count of hhs
  total_count = df.select(col_to_count).distinct().count()

  # Calculate the percentage of hhs by type
  final_df = (grouped_df
              .withColumn(f"{prefix}_percentage",
                           (f.col(f"{prefix}_hh_count") / total_count * 100)
                           .cast("double"))
  )

  return final_df

# COMMAND ----------

prefstore_dna_micro_metro_nonmetro = groupby_count_perc(metro_seg, "CBSA_TYPE", "ehhn", "prefstore_dna")
# prefstore_dna_micro_metro_nonmetro.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## state

# COMMAND ----------

#ehhn and state 
state_seg = pref_store_and_dna.select("ehhn", "STATE")
# state_seg.display()

# COMMAND ----------

prefstore_dna_state = groupby_count_perc(state_seg, "STATE", "ehhn", "prefstore_dna")
# prefstore_dna_state.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## media market

# COMMAND ----------

#ehhn and media market 
media_market_seg = pref_store_and_dna.select("ehhn", "IRI_MEDIA_MKT_NAME")
# media_market_seg.display()

# COMMAND ----------

prefstore_dna_media_market = groupby_count_perc(media_market_seg, "IRI_MEDIA_MKT_NAME", "ehhn", "prefstore_dna")
# prefstore_dna_media_market.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## census region

# COMMAND ----------

cen_region_seg = pref_store_and_dna.select("ehhn", "CEN_REG")

# COMMAND ----------

prefstore_dna_cen_region = groupby_count_perc(cen_region_seg, "CEN_REG", "ehhn", "prefstore_dna")
# prefstore_dna_cen_region.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## census division

# COMMAND ----------

cen_division_seg = pref_store_and_dna.select("ehhn", "CEN_DIV")

# COMMAND ----------

prefstore_dna_cen_division = groupby_count_perc(cen_division_seg, "CEN_DIV", "ehhn", "prefstore_dna")
# prefstore_dna_cen_division.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # customer location attributes

# COMMAND ----------

# https://confluence.kroger.com/confluence/display/8451GC/Geospatial+Data#expand-CustomerLocationAttributes

customer_loc_attributes = spark.read.format("csv").option("header", "true").option("delimiter", "|").load("abfss://landingzone@sa8451entlakegrnprd.dfs.core.windows.net/mart/geospatial/cust_geo_attributes/cust_geo_attributes_20230831.csv")

# COMMAND ----------

# emails and chat with Sarah Trefzger
# in order to get metro, micro, rural, I need to split FIPS to the first 12 digits and dedupe
# https://confluence.kroger.com/confluence/display/8451GC/Geospatial+Data#expand-GeographyRelationshipLookup

geo_lookup_table = (spark.read.format("csv").option("header", "true").option("delimiter", "|").load("abfss://landingzone@sa8451entlakegrnprd.dfs.core.windows.net/mart/geospatial/geo_relationship_lkup/")
                    .withColumn("BLOCK_GROUP", f.substring(f.col("FIPS"), 1, 12))
).dropDuplicates(["BLOCK_GROUP"])

# COMMAND ----------

# joining 

cla = ((customer_loc_attributes
        .join(geo_lookup_table.drop("IRI_MEDIA_MKT_NAME", "IRI_MEDIA_MKT_CODE" ),
              on = 'BLOCK_GROUP',
              how='left')
).join(states_info,
              customer_loc_attributes['STATE_NAME'] == states_info['NAME'], 
              how = 'left')
)

# COMMAND ----------

customer_loc_attributes.filter(f.col("ehhn") == "84112800").display()

# COMMAND ----------

cla.filter(f.col("ehhn") == "84112800").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metro, Micro, NonMetro

# COMMAND ----------

metro_seg_cla = cla.select("EHHN", "CBSA_TYPE")

# COMMAND ----------

cla_micro_metro_nonmetro = groupby_count_perc(metro_seg_cla, "CBSA_TYPE", "ehhn", "cla")
# cla_micro_metro_nonmetro.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## state

# COMMAND ----------

#ehhn and state 
state_seg_cla = cla.select("ehhn", "STUSPS")
# state_seg_cla.display()

# COMMAND ----------

cla_state = (groupby_count_perc(state_seg_cla, "STUSPS", "ehhn", "cla")
             .withColumnRenamed("STUSPS", "STATE"))
# cla_state.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## media market

# COMMAND ----------

media_market_seg_cla = cla.select("ehhn", "IRI_MEDIA_MKT_NAME")

# COMMAND ----------

cla_media_market = groupby_count_perc(media_market_seg_cla, "IRI_MEDIA_MKT_NAME", "ehhn", "cla")
# cla_media_market.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## census region

# COMMAND ----------

cen_region_seg_cla = cla.select("ehhn", "CEN_REG")

# COMMAND ----------

cla_cen_region = groupby_count_perc(cen_region_seg_cla, "CEN_REG", "ehhn", "cla")
# cla_cen_region.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## census division

# COMMAND ----------

cen_division_seg_cla = cla.select("ehhn", "CEN_DIV")

# COMMAND ----------

cla_cen_division = groupby_count_perc(cen_division_seg_cla, "CEN_DIV", "ehhn", "cla")
# cla_cen_division.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #comparisons

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Metro, Micro, NonMetro

# COMMAND ----------

mmnm = prefstore_dna_micro_metro_nonmetro.join(cla_micro_metro_nonmetro, on ="CBSA_TYPE", how = "outer")
mmnm.display()

# COMMAND ----------

# are there any ehhn in the customer location attributes that arent in the preferred store

missing_ehhn = metro_seg_cla.join(metro_seg, on="ehhn", how="left_anti")
missing_ehhn.count()

# COMMAND ----------

# is there any segment where customer location attribute ehhn outnumber perferred store/store dna

(mmnm
 .withColumn("hh_difference", f.col("prefstore_dna_hh_count") - f.col("cla_hh_count"))
 .filter(f.col("hh_difference") < 0)
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## state

# COMMAND ----------

comp_state = prefstore_dna_state.join(cla_state, on ="STATE", how = "outer")
comp_state.display()

# COMMAND ----------

# are there any ehhn in the customer location attributes that arent in the preferred store

missing_ehhn = state_seg_cla.join(state_seg, on="ehhn", how="left_anti")
missing_ehhn.count()

# COMMAND ----------

# are there any ehhn that dont match up 

joined_df = ((state_seg_cla
              .join(state_seg, on="ehhn", how="inner")
              ).withColumn("match", f.when(f.col("STUSPS") == f.col("STATE"), "Match").otherwise("Not Match"))
)

joined_df.filter(f.col("match") != "Match").display()

# COMMAND ----------

# is there any segment where customer location attribute ehhn outnumber perferred store/store dna

(comp_state
 .withColumn("hh_difference", f.col("prefstore_dna_hh_count") - f.col("cla_hh_count"))
 .filter(f.col("hh_difference") < 0)
).display()


# I bet the FL difference is from Ocado 

# COMMAND ----------

# MAGIC %md
# MAGIC ## media market

# COMMAND ----------

comp_mm = prefstore_dna_media_market.join(cla_media_market, on ="IRI_MEDIA_MKT_NAME", how = "outer")
comp_mm.display()

# COMMAND ----------

# are there any ehhn in the customer location attributes that arent in the preferred store

missing_ehhn = media_market_seg_cla.join(media_market_seg, on="ehhn", how="left_anti")
missing_ehhn.count()

# COMMAND ----------

# is there any segment where customer location attribute ehhn outnumber perferred store/store dna

(comp_mm
 .withColumn("hh_difference", f.col("prefstore_dna_hh_count") - f.col("cla_hh_count"))
 .filter(f.col("hh_difference") < 0)
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## census region

# COMMAND ----------

comp_region = prefstore_dna_cen_region.join(cla_cen_region, on ="CEN_REG", how = "outer")
comp_region.display()

# COMMAND ----------

# are there any ehhn in the customer location attributes that arent in the preferred store

missing_ehhn = cen_region_seg_cla.join(cen_region_seg, on="ehhn", how="left_anti")
missing_ehhn.count()

# COMMAND ----------

# is there any segment where customer location attribute ehhn outnumber perferred store/store dna

(comp_region
 .withColumn("hh_difference", f.col("prefstore_dna_hh_count") - f.col("cla_hh_count"))
 .filter(f.col("hh_difference") < 0)
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## census division

# COMMAND ----------

comp_division = prefstore_dna_cen_division.join(cla_cen_division, on ="CEN_DIV", how = "outer")
comp_division.display()

# COMMAND ----------

# are there any ehhn in the customer location attributes that arent in the preferred store

missing_ehhn = cen_division_seg_cla.join(cen_division_seg, on="ehhn", how="left_anti")
missing_ehhn.count()

# COMMAND ----------

# is there any segment where customer location attribute ehhn outnumber perferred store/store dna

(comp_division
 .withColumn("hh_difference", f.col("prefstore_dna_hh_count") - f.col("cla_hh_count"))
 .filter(f.col("hh_difference") < 0)
).display()

# COMMAND ----------


