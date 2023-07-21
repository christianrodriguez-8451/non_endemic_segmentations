# Databricks notebook source
"""
Reads in the commodity-segments control file and pulls in a year of the most recent
year of transaction data via ACDS. For each segmentation listed in the control file, a list
of UPCs (gtin_no) is created by pulling from ACDS all the UPCs that fall under the
given commodities and sub-commodities. PID is utilized to designate every UPC their
commodity and sub-commodity label. The final output is a delta file with gtin_no
as the only column.

Notes
  The control file is a comma separated file that contains the following
  columns: segmentation, commodities, sub-commodities, and weeks.
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

# COMMAND ----------

import config as con
import dateutil.relativedelta as dr
import datetime as dt
from effodata import ACDS, golden_rules
import pyspark.sql.functions as f
import pandas as pd

#Read in your control file (for each segment, specifies which commodities and sub-commodities it is made of)
con_fp = con.control_fp
segments_df = spark.read.csv(con_fp, sep=",", header=True)
segments_df = segments_df.toPandas()
segments_df = segments_df.loc[~segments_df["segmentation"].isna(), :]

#Process control file into dictionary for easier handling
segments_dict = {}
for n in range(0, len(list(segments_df["segmentation"]))):
  
  #Column order is assumed to be segment, commodities, sub-commodities, weeks.
  segment = segments_df.iat[n, 0]
  segment = segment.strip()
  segment = segment.lower()
  segment = segment.replace(" ", "_")
  segment = segment.replace("+", "and")

  #The columns with lists of strings are delimited by pipe and
  #commodities/sub-commodities are capitalized within PID.
  str_delim = "|"

  commodities = segments_df.iat[n, 1]
  if not commodities == None:
    commodities = commodities.split(str_delim)
    commodities = [c.strip() for c in commodities]
    commodities = [c.upper() for c in commodities]
  else:
    commodities = []

  sub_commodities = segments_df.iat[n, 2]
  if not sub_commodities == None:
    sub_commodities = sub_commodities.split(str_delim)
    sub_commodities = [c.strip("") for c in sub_commodities]
    sub_commodities = [c.upper() for c in sub_commodities]
  else:
    sub_commodities = []

  weeks =  int(segments_df.iat[n, 3])

  segments_dict[segment] = {
    "commodities": commodities,
    "sub_commodities": sub_commodities,
    "weeks": weeks,
  }
  del(segment, commodities, sub_commodities)

#Define the time range of data that you are pulling
max_weeks = 52
today = dt.date.today()
last_monday = today - dr.datetime.timedelta(days=today.weekday())
start_date = last_monday - dr.datetime.timedelta(weeks=max_weeks)
end_date = last_monday - dr.datetime.timedelta(days=1)

#Pull in transaction data
acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))
#Add in a week column in your acds pull to enable easy querying of variable weeks
acds = acds.\
withColumn('year', f.substring('trn_dt', 1, 4)).\
withColumn('month', f.substring('trn_dt', 5, 2)).\
withColumn('day', f.substring('trn_dt', 7, 2))
acds = acds.withColumn('date', f.concat(f.col('month'), f.lit("-"), f.col("day"), f.lit("-"), f.col("year")))
acds = acds.select("gtin_no", "trn_dt", f.to_date(f.col("date"), "MM-dd-yyyy").alias("transaction_date"))
acds = acds.withColumn("end_date", f.lit(end_date))
acds = acds.select("gtin_no", "trn_dt", "transaction_date", "end_date",
                   f.datediff(f.col("end_date"), f.col("transaction_date")).alias("day"))
acds = acds.withColumn("week", f.col("day")/7)
acds = acds.withColumn("week", f.ceil(f.col("week")))
acds = acds.select("gtin_no", "week")
acds = acds.dropDuplicates()

#Bump ACDS against PID to get commmodity + sub-commodity information
pid_path = "abfss://acds@sa8451posprd.dfs.core.windows.net/product/current"
pid = spark.read.parquet(pid_path)
pid = pid.select("gtin_no", "commodity_desc", "sub_commodity_desc")
pid = pid.dropDuplicates(["gtin_no"])
acds = acds.join(pid, on="gtin_no", how="inner")

#Filter for only the relevant commodities and sub-commodities to keep the dataframe small
my_comms = []
my_subs = []
for s in list(segments_dict.keys()):
  my_comms += segments_dict[s]["commodities"]
  my_subs += segments_dict[s]["sub_commodities"]

my_comms = list(set(my_comms))
my_subs = list(set(my_subs))
all_gtins = acds.filter((acds["commodity_desc"].isin(my_comms)) | (acds["sub_commodity_desc"].isin(my_subs)))
all_gtins.cache()
del(my_comms, my_subs)

#Create the UPC list for each segmentation in the control file
for s in list(segments_dict.keys()):
  #Un-pack the parameters for the given segment
  my_dict = segments_dict[s]
  comms = my_dict["commodities"]
  sub_comms = my_dict["sub_commodities"]
  n_weeks = my_dict["weeks"]

  #Filter only on the commodities and sub-commodities relevant to the given segment
  segment_gtins = all_gtins.filter((all_gtins["commodity_desc"].isin(comms)) | (all_gtins["sub_commodity_desc"].isin(sub_comms)))
  segment_gtins = segment_gtins.filter(segment_gtins["week"] <= n_weeks)
  segment_gtins = segment_gtins.select("gtin_no")
  segment_gtins = segment_gtins.dropDuplicates()
  segment_gtins.cache()
  
  #Write-out the file
  cyc_date = dt.date.today().strftime('%Y-%m-%d')
  cyc_date = "cycle_date={}".format(cyc_date)
  output_dir = con.output_fp + cyc_date
  if not (cyc_date in list(dbutils.fs.ls(con.output_fp))):
    dbutils.fs.mkdirs(output_dir)

  output_fp =  output_dir + "/" + s
  segment_gtins.write.mode("overwrite").format("delta").save(output_fp)
    
  del(comms, sub_comms, n_weeks, segment_gtins)
