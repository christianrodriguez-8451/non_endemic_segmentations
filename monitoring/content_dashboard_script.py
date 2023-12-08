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

# COMMAND ----------

# eventually read in config file

# import toolbox.egress_prism_input_data as my_config

# COMMAND ----------

# MAGIC %md
# MAGIC # set up

# COMMAND ----------

# https://github.com/christianrodriguez-8451/non_endemic_segmentations/blob/main/toolbox/egress_prism_input_data.py#L75

# error with heart friendly
# error with non veg

segmentations = {
  "funlo": [
    "free_from_gluten", "grain_free", "healthy_eating",
    #  "heart_friendly",
    "ketogenic", "kidney-friendly", "lactose_free", "low_bacteria", "paleo",
    "vegan", "vegetarian", "beveragist", "breakfast_buyers", "hispanic_cuisine",
    "low_fodmap", "mediterranean_diet", "organic", "salty_snackers"
    # "non_veg", #  "low_salt",  "low_protein", "heart_friendly", "macrobiotic",
    ],
  # "percentile": percentile_segmentations,
  # "fuel": ["gasoline", "gasoline_premium_unleaded", "gasoline_unleaded_plus", "gasoline_reg_unleaded"],?
}

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

segmentation_agg_df.write.mode("overwrite").parquet('abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/content_dashboard/segmentation_agg_df')

# COMMAND ----------

# MAGIC %md
# MAGIC # time master

# COMMAND ----------

time_master_0 = segmentation_agg_df.select(f.col("time")).distinct()

time_master_1 = time_master_0.withColumn("time", f.col("time").cast("int"))

# Order the DataFrame by the 'time' column in ascending order
time_master_2 = time_master_1.orderBy("time")


# COMMAND ----------

time_master_2.display()

# COMMAND ----------

time_master_2.write.mode("overwrite").parquet('abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/content_dashboard/time')

# COMMAND ----------

metro = spark.read.format("delta").load(f"abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/geospatial/metro_micro_nonmetro/modality=metro_micro_nonmetro")

# COMMAND ----------

help = spark.read.format("delta").load(f"abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/embedded_dimensions/customer_data_assets/segment_behavior/segmentation/modality=free_from_gluten")
help.display()

# COMMAND ----------


