# Databricks notebook source
"""
This code is ran after seasonal_spending.py.

This is the code used to identify the season's households
given the season's product list. Households are selected
by observing all purchases made in the season's product
list for the 4 days leading up the season. Purchases are
aggregated at the household level and the households
that spent more than the 25th percentile are considered
part of the season's audience.

The daily spending files from seasonal_spending.py are used
as input files for this code.
"""

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
from effodata import ACDS, golden_rules
import resources.config as config

# Initialize a Spark session
spark = SparkSession.builder.appName("seasonal_audiences").getOrCreate()

# COMMAND ----------

#Read in seasonal commodities + sub-commodities control file we have out there.
#Read in the control file and have it ready for each
#audience's commodities + sub-commodities
fp = f"abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/seasonal_commodities_control_final_2023.csv"
schema = t.StructType([
    t.StructField("holiday", t.StringType(), True),
    t.StructField("commodity", t.StringType(), True),
    t.StructField("sub_commodity", t.StringType(), True),
    t.StructField("seasonality_score", t.IntegerType(), True)
])
control = spark.read.csv(fp, schema=schema, header=True)

# COMMAND ----------

#Define season dates
season_dates = {
  "super_bowl": "20230212",
  "valentines_day": "20230214",
  "st_patricks_day": "20230317",
  "easter": "20230409",
  "may_5th": "20230505",
  "mothers_day": "20230514",
  "memorial_day": "20230529",
  "fathers_day": "20230618",
  "july_4th": "20230704",
  "back_to_school": "20230819",
  "labor_day": "20230904",
  "halloween": "20231031",
  "thanksgiving": "20231123",
  "christmas_eve": "20231224",
  "new_years_eve": "20231231",
}
today = date.today().strftime('%Y%m%d')
for s in list(season_dates.keys()):
  #Read in the audience's datasets for box-and-whiskers plots + ehhn-count line plots
  fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + s + "/" + "{}_daily_spending".format(s)
  daily_df = spark.read.format("delta").load(fp)

  #Keep only 4 days leading up to season + season's date
  s_enddate = season_dates[s]
  s_enddate = datetime.strptime(s_enddate, '%Y%m%d')
  s_startdate = s_enddate - timedelta(days=4)
  daily_df = daily_df.filter((f.col("date") >= int(s_startdate.strftime('%Y%m%d'))) & (f.col("date") <= int(s_enddate.strftime('%Y%m%d'))))
  
  #Aggregate spending to the household level
  ehhn_df = daily_df.\
  groupby("ehhn").\
  agg(
    f.sum("dollars_spent").alias("dollars_spent"),
  )

  #Calculate the 25th percentile on spending
  perc_25th = ehhn_df.approxQuantile("dollars_spent", [0.25], 0.00001)
  perc_25th = perc_25th[0]
  ehhn_df = ehhn_df.withColumn(
    "segment",
    f.when(f.col("dollars_spent") >= perc_25th, "H")
     .otherwise("M")
  )
  #Keep any ehhn w/ greater than or equal spending to the 25th percentile.
  ehhn_df = ehhn_df.filter(f.col("segment") == "H")
  ehhn_df = ehhn_df.select("ehhn", "segment")
  
  #Print raw sizing.
  message = f"{s} household count: {ehhn_df.count()}"
  print(message)
  
  #Write-out file. Print success statement.
  output_fp = f"abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/seasonal_segmentations/{s}/{s}_{today}"
  ehhn_df.write.mode("overwrite").format("delta").save(output_fp)
  message = f"SUCCESS: Wrote out {output_fp}"
  print(message)

  del(fp, daily_df, s_enddate, s_startdate, ehhn_df, message, output_fp)

