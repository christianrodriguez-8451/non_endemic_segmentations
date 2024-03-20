# Databricks notebook source
# pip install git+https://github.com/christianrodriguez-8451/non_endemic_segmentations.git

# COMMAND ----------

import toolbox
print(toolbox.__file__)

# COMMAND ----------

# common python packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from functools import reduce
from datetime import datetime, timedelta

# spark packages
import pyspark.sql.functions as f
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *

# internal 8451 packages, please search on 8451 github for reference
from effodata import ACDS, golden_rules, Sifter, Equality, Joiner
import kayday as kd
from toolbox.config import segmentation
import toolbox.config as config
# from toolbox.config import add_facebook_pandora_flags
from toolbox import config
from kpi_metrics import KPI, AliasMetric, CustomMetric, AliasGroupby, available_metrics, get_metrics
import seg
from seg.utils import DateType

# COMMAND ----------

# MAGIC %md 
# MAGIC # The Work

# COMMAND ----------

def create_df_groupby_propensity_time(segmentation:str):

  # getting the file path
  root = config.get_directory(segmentation)

  # reading in the propensity
  propensity = config.audience_dict[segmentation]["propensity_compisition"]

  # reading in the segment_type
  segment_type = config.get_type(segmentation)

  # reading in the Onsite column
  #onsite =

  #reading in offsite column 
  Offsite = config.add_facebook_pandora_flags(logger , hshd_universe_df , dig_cus_adv_pref_flag_N)
   
  # reading in the frontend_name
  frntend_name = config.audience_dict[segmentation]["frontend_name"]

  # formatting propensity for a later f.lit in the final df 
  formatting_propensity = ''.join(propensity)

  # initiating an empty df for the loop
  one_segment_master_file = None

  # loop to read in all files for one segment 
  for i in config.get_files(segmentation):

    one_file = root + i
    read_in = spark.read.format("delta").load(one_file)

    if one_segment_master_file is None:
      one_segment_master_file = read_in
    else:
      one_segment_master_file = one_segment_master_file.union(read_in)

  # filtering to just the propensities in the segment 
  one_segment_master_file_propensity = one_segment_master_file.filter(f.col("segment").isin(propensity))

  # group by date to sum ehhn by time
  # add columns to indicate segment and propensity 
  group_by = ((one_segment_master_file_propensity
              .groupBy("stratum_week")
              .agg(f.countDistinct("ehhn").alias("Household_Count"))
              ).withColumnRenamed("stratum_week", "Time")
              .withColumn("Backend_Name", f.lit(segmentation))
              .withColumn("Propensity", f.lit(formatting_propensity))
              .withColumn("Propensity_type", f.lit(segment_type))
              .withColumn("Segment", f.lit(frntend_name))
              .withColumn("Offsite_Eligibility_Count", f.lit(Offsite))
              .withColumn("Onsite_Eligibility_Count", f.lit(None))
  )

  return group_by

# COMMAND ----------

def add_facebook_pandora_flags(logger, hshd_universe_df, dig_cus_adv_pref_flag_N):
    logger.info("Adding Facebook and Pandora Flags to Weekly eligible")
    
    if "ADV_PREF_FLAG" not in hshd_universe_df.columns:
        hshd_universe_df = hshd_universe_df.withColumn("ADV_PREF_FLAG", lit("N"))
    
    if "KPM_LT_ELIGIBLE_FLAG" not in hshd_universe_df.columns:
        hshd_universe_df = hshd_universe_df.withColumn("KPM_LT_ELIGIBLE_FLAG", lit("N"))
    
    hshd_universe_df = hshd_universe_df.join(
        dig_cus_adv_pref_flag_N.withColumnRenamed("ADV_PREF_FLAG", "DIG_CUS_ADV_PREF_FLAG"),
        hshd_universe_df["EHHN"] == dig_cus_adv_pref_flag_N["EHHN"],
        "left_outer") \
        .drop(dig_cus_adv_pref_flag_N["EHHN"]) \
        .withColumn("DIG_CUS_ADV_PREF_FLAG", col("DIG_CUS_ADV_PREF_FLAG")) \
        .drop(dig_cus_adv_pref_flag_N["ADV_PREF_FLAG"]) \
        .withColumn("DIG_CUS_ADV_PREF_FLAG", expr("CASE WHEN DIG_CUS_ADV_PREF_FLAG = 'N' THEN DIG_CUS_ADV_PREF_FLAG ELSE 'Y' END")) \
        .withColumn("FACEBOOK_FLAG", 
                    expr("CASE WHEN DIG_CUS_ADV_PREF_FLAG = 'Y' AND HAVE_PII_FLAG = 'Y' AND KPM_LT_ELIGIBLE_FLAG = 'N' THEN 'Y' ELSE 'N' END")) \
        .withColumn("PANDORA_FLAG", 
                    expr("CASE WHEN DIG_CUS_ADV_PREF_FLAG = 'Y' AND HAVE_PII_FLAG = 'Y' AND KPM_LT_ELIGIBLE_FLAG = 'N' THEN 'Y' ELSE 'N' END"))
    
    hshd_universe_df = hshd_universe_df.drop("DIG_CUS_ADV_PREF_FLAG")
    
    return hshd_universe_df

# COMMAND ----------

import logging
logger = logging.getLogger()  # Assume you have a logger object

hshd_universe_df = spark.createDataFrame([(1, 'Y')], ['EHHN', 'HAVE_PII_FLAG'])  # Replace with your actual DataFrame
dig_cus_adv_pref_flag_N = spark.createDataFrame([(1, 'Y')], ['EHHN', 'ADV_PREF_FLAG'])  # Replace with your actual DataFrame

# COMMAND ----------

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit

def add_facebook_pandora_flags(logger, hshd_universe_df, dig_cus_adv_pref_flag_N):
    logger.info("Adding Facebook and Pandora Flags to Weekly eligible")
    
    if "ADV_PREF_FLAG" not in hshd_universe_df.columns:
        hshd_universe_df = hshd_universe_df.withColumn("ADV_PREF_FLAG", lit("N"))
    
    if "KPM_LT_ELIGIBLE_FLAG" not in hshd_universe_df.columns:
        hshd_universe_df = hshd_universe_df.withColumn("KPM_LT_ELIGIBLE_FLAG", lit("N"))
    
    hshd_universe_df = hshd_universe_df.join(
        dig_cus_adv_pref_flag_N.withColumnRenamed("ADV_PREF_FLAG", "DIG_CUS_ADV_PREF_FLAG"),
        hshd_universe_df["EHHN"] == dig_cus_adv_pref_flag_N["EHHN"],
        "left_outer") \
        .drop(dig_cus_adv_pref_flag_N["EHHN"]) \
        .withColumn("DIG_CUS_ADV_PREF_FLAG", col("DIG_CUS_ADV_PREF_FLAG")) \
        .drop(dig_cus_adv_pref_flag_N["ADV_PREF_FLAG"]) \
        .withColumn("DIG_CUS_ADV_PREF_FLAG", expr("CASE WHEN DIG_CUS_ADV_PREF_FLAG = 'N' THEN DIG_CUS_ADV_PREF_FLAG ELSE 'Y' END")) \
        .withColumn("FACEBOOK_FLAG", 
                    expr("CASE WHEN DIG_CUS_ADV_PREF_FLAG = 'Y' AND HAVE_PII_FLAG = 'Y' AND KPM_LT_ELIGIBLE_FLAG = 'N' THEN 'Y' ELSE 'N' END")) \
        .withColumn("PANDORA_FLAG", 
                    expr("CASE WHEN DIG_CUS_ADV_PREF_FLAG = 'Y' AND HAVE_PII_FLAG = 'Y' AND KPM_LT_ELIGIBLE_FLAG = 'N' THEN 'Y' ELSE 'N' END"))
    
    hshd_universe_df = hshd_universe_df.drop("DIG_CUS_ADV_PREF_FLAG")
    
    return hshd_universe_df

# Example usage:
logger = logging.getLogger()  # Replace with your logger object
hshd_universe_df = spark.createDataFrame([(1, 'Y')], ['EHHN', 'HAVE_PII_FLAG'])  # Replace with your actual DataFrame
dig_cus_adv_pref_flag_N = spark.createDataFrame([(1, 'Y')], ['EHHN', 'ADV_PREF_FLAG'])  # Replace with your actual DataFrame

final_df = add_facebook_pandora_flags(logger, hshd_universe_df, dig_cus_adv_pref_flag_N)
display(final_df)

# COMMAND ----------

import logging
from pyspark.sql.functions import col
logger = logging.getLogger()  # Assume you have a logger object


hshd_universe_df = spark.createDataFrame([(1, 'Y')], ['EHHN', 'HAVE_PII_FLAG'])  # Replace with your actual DataFrame
dig_cus_adv_pref_flag_N = spark.createDataFrame([(1, 'Y')], ['EHHN', 'ADV_PREF_FLAG'])  # Replace with your actual DataFrame

# COMMAND ----------

all_segment_master_file = None
# from pyspark.sql.functions import F

for i in config.segmentations.all_segmentations:
  run_segment = create_df_groupby_propensity_time(i)

  if run_segment is not None:  # Check if run_segment is not None
    if all_segment_master_file is None:
      all_segment_master_file = run_segment
    else:
      all_segment_master_file = all_segment_master_file.union(run_segment)

# COMMAND ----------

from pyspark.sql.functions import col

all_segment_master_file = None

for i in config.segmentations.all_segmentations:
  run_segment = create_df_groupby_propensity_time(i)

  if run_segment is not None:
    if all_segment_master_file is None:
      all_segment_master_file = run_segment
    else:
      all_segment_master_file = all_segment_master_file.union(run_segment)

all_segment_master_file.display()

# COMMAND ----------

if all_segment_master_file is not None:
    all_segment_master_file.display()
else:
    print("all_segment_master_file is None")

# COMMAND ----------

# MAGIC %md
# MAGIC ##write to blob

# COMMAND ----------

# # change "data" to whatever your final df ends up being!!! 
# all_segment_master_file.write.mode("overwrite").parquet('abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/powerbi_inputs')

# COMMAND ----------

#Segmentation class has metadata on each segmentaion
#data includes: name, frontend name, segment type, type, propensities,
#directory, and files
segment = config.segmentation("organic")
segment.name

# COMMAND ----------

#Name that appears on Prism UI
segment.frontend_name

# COMMAND ----------

#Group name that appears on Prism UI
segment.segment_type

# COMMAND ----------

#Ranking method used
segment.type

# COMMAND ----------

#Propensities used in live production
segment.propensities

# COMMAND ----------

#Directory that contains household files
segment.directory

# COMMAND ----------

#Household files present for given segmentation
segment.files

# COMMAND ----------

def get_frontend_name(segmentation:str):
  """
  """
  if segmentation in segmentations.audience_dict[segmentation_name]:
    frontend_name = audience_dict[segmentation_name]["frontend_name"]

  else:
    message = (
      "{} is not present in audience dicitonary contained withing the 'segmentations' class." +
      "Make sure to update the 'segmentations' class or config files as appropiate."
    )
    raise ValueError(message)

  return(frontend_name)

# COMMAND ----------

# frntend_name= get_frontend_name(segmentation)
# print(frontend_name)

# COMMAND ----------

import toolbox.config as con
import pyspark.sql.functions as f
import pyspark.sql.types as t
import datetime as dt
 
segs = con.segmentations.all_segmentations
my_schema = t.StructType([
    t.StructField("ehhn", t.StringType(), True),
    t.StructField("segment", t.StringType(), True),
    t.StructField("segmentation", t.StringType(), True),
])
df = spark.createDataFrame([], schema=my_schema)
for s in segs:
  #Get latest upc file of each segmentation
  #Also get timestamp to keep upc_lists and percentile_segmentations in sync
  segment = con.segmentation(s)
  fn = segment.files[-1]
  fp = segment.directory + fn
  temp = spark.read.format("delta").load(fp)
  temp = temp.withColumn("segmentation", f.lit(s))
  temp = temp.filter(f.col("segment").isin(segment.propensities))
  temp = temp.select("ehhn", "segment", "segmentation")
  df = df.union(temp)
 
df = df.groupBy("segmentation").count()
df.show(10, truncate=False)
