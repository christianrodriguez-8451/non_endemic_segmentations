# Databricks notebook source
"""
This code is ran after seasonal_audience_final.py.

This code is used to egress the seasons' audience files
over to On-Prem. The files are read-in from the Azure location,
they are written out to the dedicated egress directory in
parquet format, and eventually the egress moves them over
to On-Prem.

This code is a smaller and simpler version of
toolbox/egress_prism_input_data.py. This is a small bandage until
the seasonal audiences are incorporated into toolbox/config.py.
"""

# COMMAND ----------

import pyspark.sql.functions as f
import datetime as dt

# COMMAND ----------

seasons = [
  "super_bowl",
  "valentines_day",
  "st_patricks_day",
  "easter",
  "may_5th",
  "mothers_day",
  "memorial_day",
  "fathers_day",
  "july_4th",
  "back_to_school",
  "labor_day",
  "halloween",
  "thanksgiving",
  "christmas_eve",
  "new_years_eve",
]
seasonal_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/seasonal_segmentations/{}/"

# COMMAND ----------

for season in seasons:
  #Get the season's/holiday's latest file
  files = dbutils.fs.ls(seasonal_dir.format(season))
  files = [x[0] for x in files]
  files.sort()
  fp = files[-1]

  #Keep only ehhn and segment
  df = spark.read.format("delta").load(fp)
  count = df.filter(f.col("segment") == "H").count()
  print("{} household count: {}".format(season, count))
    
  #Write out to the egress directory and write out in format that engineering expects
  egress_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/egress"
  today = dt.datetime.today().strftime('%Y%m%d')
  dest_fp = egress_dir + '/' + season + '/' + season + '_' + today
  df.write.mode("overwrite").format("parquet").save(dest_fp)
  print("SUCCESS - Wrote out to {}!\n\n".format(dest_fp))

