# Databricks notebook source
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

# eventually read in config file

import toolbox.config as config

# COMMAND ----------

def create_df_groupby_propensity_time(segmentation):

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
all_segment_master_file = None

for i in config.segmentations.funlo_segmentations:

  run_segment = create_df_groupby_propensity_time(i)

  if all_segment_master_file is None:
    all_segment_master_file = run_segment
  else:
    all_segment_master_file = all_segment_master_file.union(run_segment)

# COMMAND ----------

all_segment_master_file.display()

# COMMAND ----------

readin = spark.read.format("delta").load("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/percentile_segmentations/fitness_enthusiast/fitness_enthusiast_20231115")

# COMMAND ----------

readin.display()

# COMMAND ----------

config.get_directory("fitness_enthusiast")

# COMMAND ----------

def extract_numeric_value(input_string):
    match = re.search(r'_(\d+)', input_string)
    if match:
        return match.group(1)
    else:
        return None

# COMMAND ----------

config.get_files("fitness_enthusiast")

# COMMAND ----------

print(extract_numeric_value('fitness_enthusiast_20231115/'))

# COMMAND ----------


def access_percentile_segs(segmentation):
  
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

test = access_percentile_segs("beautists")
test.display()

# COMMAND ----------

readin2 = spark.read.format("delta").load("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/fuel_segmentations/gasoline/gasoline_20231110")
readin2.display()

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

spark.read.format("delta").load("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/percentile_segmentations/casual_auto_fixers/")

# COMMAND ----------

config.segmentations.percentile_segmentations 

# COMMAND ----------

readin2 = spark.read.format("delta").load("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/fuel_segmentations/gasoline/gasoline_20231110")
readin2.display()

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

readin2 = spark.read.format("delta").load("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/fuel_segmentations/gasoline/gasoline_20231110")
readin2.display()

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

unioned_df = all_segment_master_file.union(percentile_results).union(fuel_results).union(geospatial_results)

# COMMAND ----------

readin2 = spark.read.format("delta").load("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/fuel_segmentations/gasoline/gasoline_20231110")
readin2.display()

# COMMAND ----------

segment_list = [segment for segment_group in segmentations for segment in segmentations[segment_group]]

# COMMAND ----------

print(segment_list)

# COMMAND ----------

# temp = read_and_groupby("macrobiotic")
# temp.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # loop

# COMMAND ----------

def read_and_groupby(modality):
  """
  arguments: 

  output:
  """
  read_in = spark.read.format("delta").load(f"abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/embedded_dimensions/customer_data_assets/segment_behavior/segmentation/modality={modality}")

  # Group by date and count distinct ehhn
  groupby_df = ((read_in
                    .groupBy("stratum_week", "segment")
                    .agg(f.countDistinct("ehhn").alias("hh_count"))
                    ).withColumnRenamed("stratum_week", "time")
                    .withColumnRenamed("segment", "modality")
                    .withColumn("segmentation", f.lit(f"{modality}"))
  )

  return groupby_df
  

# COMMAND ----------

# initialize an empty df to hold the appended data
segmentation_agg_df = None

for i in segment_list:

  current_df = read_and_groupby(i)

  # append the df to the master df
  if segmentation_agg_df is None:
    segmentation_agg_df = current_df

  else:
    segmentation_agg_df = segmentation_agg_df.union(current_df)

# COMMAND ----------

segmentation_agg_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##write to blob

# COMMAND ----------

all_segment_master_file.write.mode("overwrite").parquet('abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/content_dashboard/segmentation_agg_df')

# COMMAND ----------

# MAGIC %md
# MAGIC # time master

# COMMAND ----------

time_master_0 = all_segment_master_file.select(f.col("time")).distinct()

time_master_1 = time_master_0.withColumn("time", f.col("time").cast("int"))

# Order the DataFrame by the 'time' column in ascending order
time_master_2 = time_master_1.orderBy("time")


# COMMAND ----------

time_master_2.display()

# COMMAND ----------

time_master_2.write.mode("overwrite").parquet('abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/content_dashboard/time')

# COMMAND ----------


