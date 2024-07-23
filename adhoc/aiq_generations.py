# Databricks notebook source
"""
Pulls in household demographic data via AIQ and then pulls in 52 weeks of
transaction data via ACDS. Next, we do an inner merge between the
households pulled via AIQ and households pulled via ACDS. Of the households
remaining, we filter on the given generation under the "hoh_generation" column
and write out a delta file. A delta file is created for the following 
segmentations: Boommers, Generation X, Millennials, and Generation Z.
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

import pyspark.sql.functions as f
import dateutil.relativedelta as dr
import datetime as dt
#84.51 packages
import seg
from effodata import ACDS, golden_rules
#Modules within repo
import commodity_segmentations.config as con

# COMMAND ----------

#Pull latest AIQ data for generation designation at ehhn level
aiq = seg.get_seg_for_date('aiq', '20240722')
aiq = aiq.select("ehhn", "hoh_generation")
aiq = aiq.dropDuplicates(["ehhn"])

message = "Counts by generation (AIQ):\n"
print(message)
aiq.\
groupby("hoh_generation").\
agg(f.countDistinct("ehhn")).\
show(50, truncate=False)

#Keep only active shoppers (anybody who has bought anything in last 52 weeks)
max_weeks = 52
today = dt.date.today()
last_monday = today - dr.datetime.timedelta(days=today.weekday())
start_date = last_monday - dr.datetime.timedelta(weeks=max_weeks)
end_date = last_monday - dr.datetime.timedelta(days=1)

acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))
acds = acds.select("ehhn")
acds = acds.dropDuplicates()
aiq = aiq.join(acds, "ehhn", "inner")

message = "Counts by generation after merging with active Kroger shoppers:\n"
print(message)
aiq.\
groupby("hoh_generation").\
agg(f.countDistinct("ehhn")).\
show(50, truncate=False)

del(max_weeks, today, last_monday, start_date, end_date, acds)

# COMMAND ----------

#Dictionary below dictates which aiq values map to which backend names
generations_dict = {
  "BABY BOOMERS (DOB 1946-1964)": "boomers",
  "GENERATION X (DOB 1965-1979)": "gen_x",
  "MILLENNIALS (DOB 1980-1996)": "millennials",
  "GENERATION Z (DOB 1997-)": "gen_z",
}

#Assert there is not a label change for each generation
hoh_generations = aiq.select("hoh_generation").dropDuplicates().collect()
hoh_generations = [x["hoh_generation"] for x in hoh_generations]
missing = []
for i in list(generations_dict.keys()):
  try:
    assert i in hoh_generations
  except:
    missing += [i]

if len(missing) > 0:
  message = "The following labels were missing from AIQ: " + ", ".join(missing) + "."
  raise ValueError(message)
  del(message)

del(hoh_generations, missing)


# COMMAND ----------

#Creating segment column to easily leverage ***
aiq = aiq.withColumn("segment", f.lit("H"))
#Write out a delta file for each generation
for i in list(generations_dict.keys()):
  output = aiq.filter(f.col("hoh_generation") == i)
  output = output.select("ehhn", "segment")

  directory = con.output_fp
  directory = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/"
  backend_name = generations_dict[i]
  today = dt.date.today().strftime('%Y%m%d')
  fp = directory + f"aiq_generations/{backend_name}/{backend_name}_{today}"
  output.write.mode("overwrite").format("delta").save(fp)
