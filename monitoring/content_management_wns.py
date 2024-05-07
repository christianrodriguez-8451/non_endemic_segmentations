# Databricks notebook source
#Importing All the libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from functools import reduce
from datetime import datetime, timedelta
import pyspark.sql.functions as f
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
from effodata import ACDS, golden_rules, Sifter, Equality, Joiner
import kayday as kd
from toolbox.config import segmentation
from kpi_metrics import KPI, AliasMetric, CustomMetric, AliasGroupby, available_metrics, get_metrics
import seg
from seg.utils import DateType
from flowcate.files import FilePath
import toolbox.config as con
import pyspark.sql.functions as f
import pyspark.sql.types as t
import datetime as dt


# COMMAND ----------

#Getting Eligiblity Table

eligibility_path = FilePath('abfss://landingzone@sa8451entlakegrnprd.dfs.core.windows.net/mart/comms/prd/fact/eligibility_fact')
latest_eligibility_df = spark.read.parquet(eligibility_path.find_latest_file()) #This will give us the Dates we are using in.
print(eligibility_path.find_latest_file())


# COMMAND ----------

# MAGIC %md
# MAGIC # The Work

# COMMAND ----------

# MAGIC %md
# MAGIC ##write to blob
# MAGIC
# MAGIC
# MAGIC ### these are the history - not new;
# MAGIC
# MAGIC ### 1) rerun with our eligibilities filter;
# MAGIC ### 2) build new ()

# COMMAND ----------

def get_all_segmentation_name():
    '''
    Get all the Segmentation names
    
    '''
    segmentation_names = con.segmentations.all_segmentations

    return segmentation_names

# COMMAND ----------

def get_all_columns(segmentation_names):
      '''
      Input: Segmentation names
      Output: Final Dataframe with all segmentation

      '''

    my_schema = t.StructType([
          t.StructField("EHHN", t.StringType(), True),
          t.StructField("SEGMENT", t.StringType(), True),
          t.StructField("SEGMENTATION", t.StringType(), True),
          t.StructField("FRONTEND_NAME", t.StringType(), True),
          t.StructField("PROPENSITY", t.StringType(), True),
          t.StructField("SEGMENT_TYPE", t.StringType(), True),
          t.StructField("PERCENTILE_SEGMENT", t.StringType(), True),
      ])
    df = spark.createDataFrame([], schema=my_schema)

    for segmentation in segmentation_names:
          segment = con.segmentation(segmentation)
          frontend_name = segment.frontend_name
          propensities = segment.propensities
          final_propensity = "".join(propensities)
          type_of_segment = segment.segment_type
          percentile_of_segment = segment.type
          latest_file = segment.files[-1]
          reading_file = segment.directory + latest_file
          segment_file = spark.read.format("delta").load(reading_file)

          segment_file = (segment_file
                  .withColumn("SEGMENTATION", f.lit(segmentation))
                  .withColumn('FRONTEND_NAME', f.lit(frontend_name))
                  .withColumn("PROPENSITY", f.lit(final_propensity))
                  .withColumn("SEGMENT_TYPE", f.lit(type_of_segment))
                  .withColumn("PERCENTILE_SEGMENT", f.lit(percentile_of_segment))
          )
          segment_file = segment_file.filter(f.col("SEGMENT").isin(segment.propensities))
          segment_file = segment_file.select("EHHN", "SEGMENT", "SEGMENTATION","FRONTEND_NAME","PROPENSITY", "SEGMENT_TYPE","PERCENTILE_SEGMENT")
          df = df.union(segment_file)
 
    return df

# COMMAND ----------

segmentation_name = get_all_segmentation_name()
final_df = get_all_columns(segmentation_names=segmentation_name)

# COMMAND ----------

display(final_df.sort('EHHN'))

# COMMAND ----------

!git merge main

# COMMAND ----------

## Frank's Edits
### need to rename df, this is too generic for troubleshooting later and might overwrite things that were named df before;

#Heather's Old Logic (2+ year old)
# select divis,
# kpm_two,
# EM_ELIG,
# ecommerce_flag,
# count(distinct ehhn)
# from (select a.*, case when tdc_eligible_flag = 'Y' or sse_eligible_flag = 'Y' or ffd_eligible_flag = 'Y' or email_eligible_flag ='Y' or cba_eligible_flag ='Y' or
# mobile_eligible_flag='Y' or has_digital_account_flag='Y' or pinterest_eligible_flag ='Y' then 'Y' else 'N' end as KPM_ELIG,
# case when sse_eligible_flag = 'Y' or ffd_eligible_flag = 'Y' then 'Y' else 'N' end as EM_ELIG,
# case when preferred_store_division is null or preferred_store_division = 'DEFAULT_HEIRARCHY' then last_store_shop_divison else preferred_store_division end as divis,
# case when facebook_flag ='Y' or ecommerce_flag='Y' or sse_eligible_flag = 'Y' or ffd_eligible_flag = 'Y' then 'Y' else 'N' end as kpm_two
# from  MKT_CM.ELIGIBILITY_FACT a
# where date_id = to_date(20220206,'YYYYMMDD')
# and last_shop_date >= to_date(20200210,'YYYYMMDD'))
 
# group by divis,
# kpm_two,
# EM_ELIG,
# ecommerce_flag;
# Heather's logic:

## put these in the story:

# Heather's Comments: 1) we need to X.98 for the result we get for the reason of LT holdout
# we dont usually (we never?) target a shopper that has not been in a Kroger store in the past year - if last shoppeed date is outside of the the current year, we deem the customer as non-active/non-eligible (Frank cannot get to this)

eligibility_analytical_dataset = final_df.join(latest_eligibility_df, on="ehhn", how='left')
### eligibility flags (onsite, offsite, overall)

### overall logic:

from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType

# data = [("John", "Y", "N", "Y"),
#         ("Alice", "N", "N", "N"),
#         ("Bob", "Y", "Y", "Y")]

# # Create DataFrame
# df = spark.createDataFrame(data, ["Name", "Col1", "Col2", "Col3"])

column_names = ['NATIVE_ELIGIBLE_FLAG','TDC_ELIGIBLE_FLAG','SSE_ELIGIBLE_FLAG','FFD_ELIGIBLE_FLAG','EMAIL_ELIGIBLE_FLAG','PUSH_FLAG',
                'FACEBOOK_FLAG','PANDORA_FLAG','CHICORY_FLAG','PUSH_FLAG','PREROLL_VIDEO_ELIGIBLE_FLAG','PINTEREST_ELIGIBLE_FLAG','ROKU_FLAG']

# Function to check if any column contains 'Y'
def any_column_contains_Y(*args):
    return 'Y' in args

# Register UDF
contains_Y_udf = udf(any_column_contains_Y, BooleanType())

# Apply UDF to create a new column


eligibility_with_on_off_overall_flag = eligibility_analytical_dataset.withColumn('onsite_flag', f.when(((f.col('TDC_ELIGIBLE_FLAG') == 'Y') | (f.col('SSE_ELIGIBLE_FLAG') == 'Y') | (f.col('NATIVE_ELIGIBLE_FLAG') == 'Y')|(f.col('EMAIL_ELIGIBLE_FLAG') == 'Y') | (f.col('PUSH_FLAG') == 'Y')), '1').otherwise('0').cast('integer'))\
                  .withColumn('offsite_flag', f.when(((f.col('FACEBOOK_FLAG') == 'Y') | (f.col('PANDORA_FLAG') == 'Y') | (f.col('PINTEREST_ELIGIBLE_FLAG') =='Y') | (f.col('PREROLL_VIDEO_ELIGIBLE_FLAG') == 'Y') | (f.col('ROKU_FLAG') == 'Y') | (f.col('CHICORY_FLAG') == 'Y')), '1').otherwise('0').cast('integer'))\
                  .withColumn("Overall Eligibility", contains_Y_udf(*[col(column) for column in column_names]))
                  

# COMMAND ----------

display(eligibility_with_on_off_overall_flag.limit(5)
        )

# COMMAND ----------

display(eligibility_with_on_off_overall_flag.groupBy('SEGMENTATION')
        .agg(f.count('EHHN').alias("count_hh"),
             f.max("frontend_name").alias("Frontend_name"),
             f.max("SEGMENT").alias("segment_extra"),
             f.max("PROPENSITY").alias("Propensity"),
             f.max("SEGMENT_TYPE").alias("Segment_type"),
             f.max("PERCENTILE_SEGMENT").alias("percentil_seg"),
             f.sum('onsite_flag').alias('onsite_count'),
             f.sum('offsite_flag').alias('offsite_count')
))

# COMMAND ----------

getting_count = (final_df.join(elig, on="ehhn", how='left')
                  .withColumn('onsite_flag', f.when(((f.col('TDC_ELIGIBLE_FLAG') == 'Y') | (f.col('SSE_ELIGIBLE_FLAG') == 'Y') | (f.col('NATIVE_ELIGIBLE_FLAG') == 'Y') |(f.col('EMAIL_ELIGIBLE_FLAG') == 'Y') | (f.col('PUSH_FLAG') == 'Y')), '1').otherwise('0').cast('integer'))
                  .withColumn('offsite_flag', f.when(((f.col('FACEBOOK_FLAG') == 'Y') | (f.col('PANDORA_FLAG') == 'Y') | (f.col('PINTEREST_ELIGIBLE_FLAG') =='Y') | (f.col('PREROLL_VIDEO_ELIGIBLE_FLAG') == 'Y') | (f.col('ROKU_FLAG') == 'Y') | (f.col('CHICORY_FLAG') == 'Y')), '1').otherwise('0').cast('integer'))
                  
                  )

# COMMAND ----------

ehhn_invalid = (getting_count
        .filter((f.col('onsite_flag') == '0') & (f.col('offsite_flag') == '0'))
)


# COMMAND ----------

segmentation_name = con.segmentations.all_segmentations
segmentation_name.sort()
problem_segmentation_name =[]
seg = segmentation_name[0]
segment = con.segmentation(seg)
file = segment.files
prope = segment.propensities
dirr = segment.directory
fornt = segment.frontend_name
seggg = segment.segment_type
tt = segment.type

# COMMAND ----------

times = []
for i in file:
  names = i[0:-1]
  times.append(names)
times

# COMMAND ----------

times[-1]

# COMMAND ----------

prope

# COMMAND ----------

dirr

# COMMAND ----------

fornt

# COMMAND ----------

seggg

# COMMAND ----------

tt
