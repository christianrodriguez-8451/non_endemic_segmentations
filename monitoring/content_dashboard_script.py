# Databricks notebook source
# MAGIC %md 
# MAGIC #Set up

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

# internal packages
from effodata import ACDS, golden_rules, Sifter, Equality, Joiner
import kayday as kd
from kpi_metrics import KPI, AliasMetric, CustomMetric, AliasGroupby, available_metrics, get_metrics
import seg
from seg.utils import DateType

import re


# COMMAND ----------

# https://github.com/christianrodriguez-8451/non_endemic_segmentations/blob/main/toolbox/config.py

import toolbox.config as config

# COMMAND ----------

# MAGIC %md 
# MAGIC # function

# COMMAND ----------

def extract_numeric_value(input_string):
  """
  Extracts and returns the numeric value following an underscore '_' in the input string.

  Parameters:
  - input_string (str): The input string from which to extract the numeric value.

  Returns:
  - str or None: If a numeric value following an underscore is found, it is returned as a string.
                  If no such value is found, returns None.
  """ 
  
  match = re.search(r'_(\d+)', input_string)
  if match:
    return match.group(1)
  else:
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC # funlo segmentations

# COMMAND ----------

# taking a look at what the funlo files look like 
reading = spark.read.format("delta").load("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/embedded_dimensions/customer_data_assets/segment_behavior/segmentation/modality=free_from_gluten/stratum_week=20230701")
reading.display()

# COMMAND ----------

def create_df_groupby_propensity_time(segmentation):
  """
  Create a grouped DataFrame by stratum_week with aggregated household counts
  for a specific segment and its associated propensities over time.

  Parameters:
  - segmentation (str): The segment identifier for which the DataFrame is created.

  Returns:
  - DataFrame: A DataFrame grouped by date with columns indicating the time,
                segment, and propensity, along with the aggregated household counts.
  """

  # getting the file path
  root = config.get_directory(segmentation)

  # reading in the propensity
  propensity = config.get_propensity_composition(segmentation)

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
              .agg(f.countDistinct("ehhn").alias("hh_count"))
              ).withColumnRenamed("stratum_week", "time")
              .withColumn("segment", f.lit(segmentation))
              .withColumn("propensity", f.lit(formatting_propensity))
  )

  return group_by

# COMMAND ----------

# initiating an empty df for the loop
funlo_segment_master_file = None

for i in config.segmentations.funlo_segmentations:

  run_segment = create_df_groupby_propensity_time(i)

  if funlo_segment_master_file is None:
    funlo_segment_master_file = run_segment
  else:
    funlo_segment_master_file = funlo_segment_master_file.union(run_segment)

# COMMAND ----------

funlo_segment_master_file.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # percentile segmentations

# COMMAND ----------

# taking a look at percentile segmentation delta structure

reading = spark.read.format("delta").load("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/percentile_segmentations/fitness_enthusiast/fitness_enthusiast_20231115")
reading.display()

# COMMAND ----------

def access_percentile_segs(segmentation):
  """
  Access and aggregate data for a specific segment based on percentiles, considering date information.

  Parameters:
  - segmentation (str): The segment identifier for which the DataFrame is accessed.

  Returns:
  - DataFrame: A DataFrame grouped by date with columns indicating the time,
                segment, and propensity, along with the aggregated household counts.

  Dependencies:
  - The function relies on configuration details obtained through the config module,
    including directory paths, propensity compositions, and file details.
  - The function also depends on the availability of the `extract_numeric_value` function
    for extracting date information from file names.
  """

  # getting the file path
  root = config.get_directory(segmentation)

  # reading in the propensity
  propensity = config.get_propensity_composition(segmentation)

  # formatting propensity for a later f.lit in the final df 
  formatting_propensity = ''.join(propensity)

  # initiating an empty df for the loop
  one_segment_master_file = None

  # loop to read in all files for one segment 
  for i in config.get_files(segmentation):

    one_file = root + i
    read_in = spark.read.format("delta").load(one_file)

    # for the percentiles segs, I need to read date from ""fitness_enthusiast_20231115/" or "beautists_20231115"
    date_string = extract_numeric_value(i)
    
    read_in_1_date = read_in.withColumn("date", f.lit(date_string))

    if one_segment_master_file is None:
      one_segment_master_file = read_in_1_date
    else:
      one_segment_master_file = one_segment_master_file.union(read_in_1_date)
  
  # filtering to just the propensities in the segment 
  one_segment_master_file_propensity = one_segment_master_file.filter(f.col("segment").isin(propensity))

  # group by date to sum ehhn by time
  # add columns to indicate segment and propensity 
  group_by = ((one_segment_master_file_propensity
              .groupBy("date")
              .agg(f.countDistinct("ehhn").alias("hh_count"))
              ).withColumnRenamed("date", "time")
              .withColumn("segment", f.lit(segmentation))
              .withColumn("propensity", f.lit(formatting_propensity))
  )

  return group_by

# COMMAND ----------

# initiating an empty df for the loop
percentile_results = None

for i in config.segmentations.percentile_segmentations :

  run_segment = access_percentile_segs(i)

  if percentile_results is None:
    percentile_results = run_segment
  else:
    percentile_results = percentile_results.union(run_segment)

# COMMAND ----------

percentile_results.display()

# COMMAND ----------

config.get_directory("casual_auto_fixers")

# COMMAND ----------

# what does auto fixers look like 

# taking a look at percentile segmentation delta structure

reading = spark.read.format("delta").load("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/percentile_segmentations/casual_auto_fixers/casual_auto_fixers_20231222")
reading.display()

# COMMAND ----------

# thankfully casual auto fixers is the last item in the list

config.segmentations.percentile_segmentations 

# COMMAND ----------

# MAGIC %md
# MAGIC #fuel segmentations

# COMMAND ----------

reading = spark.read.format("delta").load("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/fuel_segmentations/gasoline/gasoline_20231110")
reading.display()

# COMMAND ----------

# initiating an empty df for the loop
fuel_results = None

for i in config.segmentations.fuel_segmentations:

  run_segment = access_percentile_segs(i)

  if fuel_results is None:
    fuel_results = run_segment
  else:
    fuel_results = fuel_results.union(run_segment)

# COMMAND ----------

fuel_results.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # geospatial segmentations

# COMMAND ----------

# inspecting how geospatial looks 

reading = spark.read.format("delta").load("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/geospatial/roadies/roadies_20231117")
reading.display()

# COMMAND ----------

# initiating an empty df for the loop
geospatial_results = None

for i in config.segmentations.geospatial_segmentations:

  run_segment = access_percentile_segs(i)

  if geospatial_results is None:
    geospatial_results = run_segment
  else:
    geospatial_results = geospatial_results.union(run_segment)

# COMMAND ----------

geospatial_results.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # union

# COMMAND ----------

unioned_df = funlo_segment_master_file.union(percentile_results).union(fuel_results).union(geospatial_results)

# COMMAND ----------

total_seg = unioned_df.select(f.col("segment")).distinct().count()
print(f"Total segmentations in the dashboard is: {total_seg}")

# COMMAND ----------

unioned_df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #write to blob

# COMMAND ----------

unioned_df.write.mode("overwrite").parquet('abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/content_dashboard/segmentation_agg_df')

# COMMAND ----------

# MAGIC %md
# MAGIC # time df

# COMMAND ----------

time_master_0 = unioned_df.select(f.col("time")).distinct()

time_master_1 = time_master_0.withColumn("time", f.col("time").cast("int"))

# Order the DataFrame by the 'time' column in ascending order
time_master_2 = time_master_1.orderBy("time")


# COMMAND ----------

time_master_2.display()

# COMMAND ----------

time_master_2.write.mode("overwrite").parquet('abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/content_dashboard/time')

# COMMAND ----------

# MAGIC %md
# MAGIC #scratch

# COMMAND ----------

lowcount = spark.read.format("delta").load("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/embedded_dimensions/customer_data_assets/segment_behavior/segmentation/modality=free_from_gluten/stratum_week=20231202")

# COMMAND ----------

# i want to verify the groupby really is seeing less than 100 H ehhn 

lowcount.filter(f.col("segment") == "H").display()

# COMMAND ----------

unioned_df.filter(f.col("hh_count") <= 500).display()

# COMMAND ----------


