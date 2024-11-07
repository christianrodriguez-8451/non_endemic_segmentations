# Databricks notebook source
"""
This code is used to take the seasonal commodities/sub-commodities
control file that is outputted from seasonal_outlier_detection.py
and apply the recommendations provided by the account managers.
The account managers reviewed the holidays in batches. The batches
are specified.

  batch 1 - Super Bowl, Valentine's Day, St. Patrick's Day

A new control file is outputted in CSV format. After the new control
file has been outputted, make sure to run seasonal_spending.py.
"""

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t
import os

# Initialize a Spark session
spark = SparkSession.builder.appName("seasonal_control_file_adjustments").getOrCreate()

# COMMAND ----------

#Read in control file with chosen commodities + sub-commodities per holiday
#This is the initial list that DSR settled with after reviewing the list outputted
#from seasonal_outlier_detection.py.
fp = (
  "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" +
  "seasonal_commodities_control2.csv"
)
schema = t.StructType([
    t.StructField("holiday", t.StringType(), True),
    t.StructField("commodity", t.StringType(), True),
    t.StructField("sub_commodity", t.StringType(), True),
    t.StructField("seasonality_score", t.IntegerType(), True)
])
original_control = spark.read.csv(fp, schema=schema, header=True)
original_control.limit(100).display()

# COMMAND ----------

#This is the first batch of holidays that the AMs reviewed and got back to DSR.
#The holidays they reviewed in this batch are Super Bowl, Valentine's Day and
#St. Patrick's Day.
fp = (
  "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" +
  "seasonal_commodities_control_reviewed1.csv"
)
schema = t.StructType([
    t.StructField("holiday", t.StringType(), True),
    t.StructField("commodity", t.StringType(), True),
    t.StructField("sub_commodity", t.StringType(), True),
    t.StructField("seasonality_score", t.IntegerType(), True)
])
reviewed_control = spark.read.csv(fp, schema=schema, header=True)

#Drop the Super Bowl, Valentine's Day, and St. Patrick's Day entries already present
#in the original control file.
new_control = original_control.filter(~f.col("holiday").isin(["super_bowl", "valentines_day", "st_patricks_day"]))
#Append what the AMs got back to DSR.
new_control = new_control.union(reviewed_control)

# COMMAND ----------

#Write out the new control file.
def write_out(df, fp, delim=",", fmt="csv"):
  """Writes out PySpark dataframe as a csv file
  that can be downloaded for Azure and loaded into
  Excel very easily.

  Example
  ----------
    write_out(df, "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/data_analysis.csv")

  Parameters
  ----------
  df: pyspark.sql.dataframe.DataFrame
    PySpark dataframe contains the data we'd like
    to conduct the group-by count on.

  fp: str
    String that the defines the column name of the 
    column of interest.

  delim: str
    String that specifies which delimiter to use in the
    write-out. Default value is ','.

  fmt: str
    String that specifies which format to use in the
    write-out. Default value is 'csv'.

  Returns
  ----------
  None. File is written out specified Azure location.
  """
  #Placeholder filepath for intermediate processing
  temp_target = os.path.dirname(fp) + "/" + "temp"

  #Write out df as partitioned file. Write out ^ delimited
  df.coalesce(1).write.options(header=True, delimiter=delim).mode("overwrite").format(fmt).save(temp_target)

  #Copy desired file from parititioned directory to desired location
  temporary_fp = os.path.join(temp_target, dbutils.fs.ls(temp_target)[3][1])
  dbutils.fs.cp(temporary_fp, fp)
  dbutils.fs.rm(temp_target, recurse=True)

output_fp = f"abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/seasonal_commodities_control_final_2023.csv"
write_out(new_control, output_fp)
