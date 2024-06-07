# Databricks notebook source
"""
Creates segmentations by using the upc lists outputted by
commodity_segmentations.py and calculating how much each
household spends on those upc lists. Households that spent
above the 66 percentile are given H propensity. Households
that spent below the 33rd percentile are given L propensity.
Otherwise, households are given M propensity.
"""

# COMMAND ----------

#When you use a job to run your notebook, you will need the service principles
#You only need to define what storage accounts you are using

#Define service principals
service_credential = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-pw')
service_application_id = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-app-id')
directory_id = "5f9dc6bd-f38a-454a-864c-c803691193c5"
#storage_account = 'sa8451dbxadhocprd'
storage_account = 'sa8451posprd'

#Set configurations
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

## Loading External Packages
import pyspark.sql.functions as f
import pyspark.sql.types as t
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window

## Loading Internal Packages
import config as con
from effodata import ACDS, golden_rules, joiner, sifter, join_on
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("percentile_propensity").getOrCreate()
segs = con.percentile_segmentations

#Define the time range of data that you are pulling
max_weeks = 52
today = date.today()
last_monday = today - timedelta(days=today.weekday())
start_date = last_monday - timedelta(weeks=max_weeks)
end_date = last_monday - timedelta(days=1)

#Pull in transaction data and keep only ehhn, gtin_no, and dollars_spent
acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))
acds = acds.select("ehhn", "gtin_no", "net_spend_amt")
#Get rid of return transactions
acds = acds.filter(f.col("net_spend_amt") > 0)

#Read in each segment's upc list and merge against ACDS
my_schema = t.StructType([
    t.StructField("gtin_no", t.StringType(), True),
    t.StructField("segmentation", t.StringType(), True)
])
seg_upcs = spark.createDataFrame([], schema=my_schema)
seg_timestamps = {}
for s in segs:
  #Get latest upc file of each segmentation
  #Also get timestamp to keep upc_lists and percentile_segmentations in sync
  seg_dir = f"{con.output_fp}upc_lists/{s}/"
  dir_contents = dbutils.fs.ls(seg_dir)
  dir_contents = [x[0] for x in dir_contents if s in x[1]]
  dir_contents.sort()
  seg_fp = dir_contents[-1]
  seg_timestamp = seg_fp.split(f"{s}_")
  seg_timestamp = seg_timestamp[1]
  seg_timestamp = seg_timestamp.strip("/")
  seg_timestamps[s] = seg_timestamp

  df = spark.read.format("delta").load(seg_fp)
  df = df.withColumn("segmentation", f.lit(s))
  seg_upcs = seg_upcs.union(df)

#For each segment, see how much each household spent
dollars_spent = acds.join(seg_upcs, on="gtin_no", how="inner")
dollars_spent = dollars_spent.withColumn("net_spend_amt", f.col("net_spend_amt").cast("float"))
dollars_spent = dollars_spent.groupBy("segmentation", "ehhn").agg(f.sum("net_spend_amt").alias("dollars_spent"))
dollars_spent.cache()

# COMMAND ----------

for s in segs:
  #Slice out each segmentation and get 33rd/66th percentiles
  df = dollars_spent.filter(dollars_spent["segmentation"] == s)
  percentiles = df.approxQuantile("dollars_spent", [0.33, 0.66], 0.00001)
  df = df.withColumn(
    "segment",
    f.when(f.col("dollars_spent") < percentiles[0], "L")
        .otherwise(f.when(f.col("dollars_spent") > percentiles[1], "H")
                  .otherwise("M"))
  )

  #Do quick-and-dirty grouped-by count for validation
  temp = df.groupby("segment").agg(f.count("*").alias("count"))
  print(f"\n{s} counts by propensity:\n")
  print(temp.show(5, truncate=False))

  #Write-out
  today = seg_timestamps[s]
  df = df.select("ehhn", "segment", "dollars_spent")
  fp = f'{con.output_fp}percentile_segmentations/{s}/{s}_{today}'
  df.write.mode("overwrite").format("delta").save(fp)
