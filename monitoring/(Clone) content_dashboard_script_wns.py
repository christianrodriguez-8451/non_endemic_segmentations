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
  Offsite = config.add_facebook_pandora_flags(hshd_universe_df)
   
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

def create_df_groupby_propensity_time(segmentation:str):

  # getting the file path
  root = config.get_directory(segmentation)

  # reading in the propensity
  propensity = config.audience_dict[segmentation]["propensity_compisition"]

  # reading in the segment_type
  segment_type = config.get_type(segmentation)

  # reading in the frontend_name
  frntend_name = config.audience_dict[segmentation]["frontend_name"]

  # formatting propensity for a later f.lit in the final df 
  formatting_propensity = ''.join(propensity)

  #reading in offsite column 
  Offsite = config.add_facebook_pandora_flags(hshd_universe_df)

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
  # check if "stratum_week" column exists
  if "stratum_week" in one_segment_master_file_propensity.columns:
    # group by "stratum_week" and calculate count distinct of "ehhn"
    group_by = (one_segment_master_file_propensity
                .groupBy("stratum_week")
                .agg(f.countDistinct("ehhn").alias("Household_Count"))
                .withColumnRenamed("stratum_week", "Time")
                .withColumn("Backend_Name", f.lit(segmentation))
                .withColumn("Propensity", f.lit(formatting_propensity))
                .withColumn("Propensity_type", f.lit(segment_type))
                .withColumn("Segment", f.lit(frntend_name))
                .withColumn("Offsite_Eligibility_Count", f.lit(Offsite))
                .withColumn("Onsite_Eligibility_Count", f.lit(None))
                )
    return group_by
  else:
    return None

# COMMAND ----------

all_segment_master_file = None

for i in config.segmentations.all_segmentations:
  run_segment = create_df_groupby_propensity_time(i)

  if run_segment is not None:  # Check if run_segment is not None
    if all_segment_master_file is None:
      all_segment_master_file = run_segment
    else:
      all_segment_master_file = all_segment_master_file.union(run_segment)

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
