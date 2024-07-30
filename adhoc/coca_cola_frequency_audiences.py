# Databricks notebook source
import dateutil.relativedelta as dr
import datetime as dt
from effodata import ACDS, golden_rules
import pyspark.sql.functions as f
import pyspark.sql.types as t
import pandas as pd
import math as m
from pyspark.sql.window import Window
import os

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

# COMMAND ----------

#########################################################
###Red Label Coca Cola
#########################################################

#my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/coca_cola_frequency/"

#1) Rejectors: Over the last 52 weeks, have purchased Carbonated Soft Drinks at least twice every 30 days,
#but have not purchased Coca Cola TM.
#in_fn = "carbonated_soft_drinks_upcs2.csv"
#notin_fn = "coca_cola_tm_upcs.csv"

#max_weeks = 13
#freq_min = 2
#freq_max = None
#cycle_length = 30

#output_fn = "{}week_".format(max_weeks) + "rejectors_hhs"

#2) Neutrals: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than twice, every 90 days.
#in_fn = "coca_cola_tm_upcs.csv"
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = 2
#cycle_length = 90

#output_fn = "{}week_".format(max_weeks) + "neutrals_hhs"

#3) Intenders: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than 3 times, every 30 days.
#in_fn = "coca_cola_tm_upcs.csv"
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = 3
#cycle_length = 30

#output_fn = "{}week_".format(max_weeks) + "intenders_hhs"

#4) Weekly+ Single-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Single-Serve at least once every 7 days.
#in_fn = "coca_cola_tm_single_serve_upcs.csv"
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = None
#cycle_length = 7

#output_fn = "{}week_".format(max_weeks) + "weekly_single_serve_hhs"

#5) Weekly+ Multi-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Multi-Serve at least once every 14 days.
#in_fn = "coca_cola_tm_multi_serve_upcs.csv"
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = None
#cycle_length = 14

#output_fn = "{}week_".format(max_weeks) + "weekly_multi_serve_hhs"

#########################################################
###Diet Coke
#########################################################

#brand = "diet_coke"
#my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/{}_frequency/".format(brand)

#1) Rejectors: Over the last 52 weeks, have purchased Carbonated Soft Drinks at least twice every 30 days,
#but have not purchased Coca Cola TM.
#in_fn = "carbonated_soft_drinks_no_{}_upcs.csv".format(brand)
#notin_fn = "{}_tm_upcs.csv".format(brand)

#max_weeks = 52
#freq_min = 2
#freq_max = None
#cycle_length = 30

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "rejectors_hhs"

#2) Neutrals: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than twice, every 90 days.
#in_fn = "{}_tm_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = 2
#cycle_length = 90

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "neutrals_hhs"

#3) Intenders: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than 3 times, every 30 days.
#in_fn = "{}_tm_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = 3
#cycle_length = 30

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "intenders_hhs"

#4) Weekly+ Single-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Single-Serve at least once every 7 days.
#in_fn = "{}_tm_single_serve_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = None
#cycle_length = 7

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "weekly_single_serve_hhs"

#5) Weekly+ Multi-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Multi-Serve at least once every 14 days.
#in_fn = "{}_tm_multi_serve_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = None
#cycle_length = 14

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "weekly_multi_serve_hhs"

#########################################################
###Coke Zero
#########################################################

#brand = "coke_zero"
#my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/{}_frequency/".format(brand)

#1) Rejectors: Over the last 52 weeks, have purchased Carbonated Soft Drinks at least twice every 30 days,
#but have not purchased Coca Cola TM.
#in_fn = "carbonated_soft_drinks_no_{}_upcs.csv".format(brand)
#notin_fn = "{}_tm_upcs.csv".format(brand)

#max_weeks = 52
#freq_min = 2
#freq_max = None
#cycle_length = 30

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "rejectors_hhs"

#2) Neutrals: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than twice, every 90 days.
#in_fn = "{}_tm_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = 2
#cycle_length = 90

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "neutrals_hhs"

#3) Intenders: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than 3 times, every 30 days.
#in_fn = "{}_tm_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = 3
#cycle_length = 30

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "intenders_hhs"

#4) Weekly+ Single-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Single-Serve at least once every 7 days.
#in_fn = "{}_tm_single_serve_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = None
#cycle_length = 7

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "weekly_single_serve_hhs"

#5) Weekly+ Multi-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Multi-Serve at least once every 14 days.
#in_fn = "{}_tm_multi_serve_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = None
#cycle_length = 14

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "weekly_multi_serve_hhs"

#########################################################
###Sprite
#########################################################

#brand = "sprite"
#my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/{}_frequency/".format(brand)

#1) Rejectors: Over the last 52 weeks, have purchased Carbonated Soft Drinks at least twice every 30 days,
#but have not purchased Coca Cola TM.
#in_fn = "carbonated_soft_drinks_no_{}_upcs.csv".format(brand)
#notin_fn = "{}_tm_upcs.csv".format(brand)

#max_weeks = 52
#freq_min = 2
#freq_max = None
#cycle_length = 30

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "rejectors_hhs"

#2) Neutrals: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than twice, every 90 days.
#in_fn = "{}_tm_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = 2
#cycle_length = 90

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "neutrals_hhs"

#3) Intenders: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than 3 times, every 30 days.
#in_fn = "{}_tm_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = 3
#cycle_length = 30

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "intenders_hhs"

#4) Weekly+ Single-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Single-Serve at least once every 7 days.
#in_fn = "{}_tm_single_serve_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = None
#cycle_length = 7

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "weekly_single_serve_hhs"

#5) Weekly+ Multi-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Multi-Serve at least once every 14 days.
#in_fn = "{}_tm_multi_serve_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = None
#cycle_length = 14

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "weekly_multi_serve_hhs"

#########################################################
###Fanta
#########################################################

#brand = "fanta"
#my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/{}_frequency/".format(brand)

#1) Rejectors: Over the last 52 weeks, have purchased Carbonated Soft Drinks at least twice every 30 days,
#but have not purchased Coca Cola TM.
#in_fn = "carbonated_soft_drinks_no_{}_upcs.csv".format(brand)
#notin_fn = "{}_tm_upcs.csv".format(brand)

#max_weeks = 13
#freq_min = 2
#freq_max = None
#cycle_length = 30

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "rejectors_hhs"

#2) Neutrals: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than twice, every 90 days.
#in_fn = "{}_tm_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = 2
#cycle_length = 90

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "neutrals_hhs"

#3) Intenders: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than 3 times, every 30 days.
#in_fn = "{}_tm_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = 3
#cycle_length = 30

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "intenders_hhs"

#4) Weekly+ Single-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Single-Serve at least once every 7 days.
#in_fn = "{}_tm_single_serve_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = None
#cycle_length = 7

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "weekly_single_serve_hhs"

#5) Weekly+ Multi-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Multi-Serve at least once every 14 days.
#in_fn = "{}_tm_multi_serve_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = None
#cycle_length = 14

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "weekly_multi_serve_hhs"

#########################################################
###Minute Maid
#########################################################

#brand = "minute_maid"
#my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/{}_frequency/".format(brand)

#1) Rejectors: Over the last 52 weeks, have purchased Carbonated Soft Drinks at least twice every 30 days,
#but have not purchased Coca Cola TM.
#in_fn = "carbonated_soft_drinks_no_{}_upcs.csv".format(brand)
#notin_fn = "{}_tm_upcs.csv".format(brand)

#max_weeks = 52
#freq_min = 2
#freq_max = None
#cycle_length = 30

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "rejectors_hhs"

#2) Neutrals: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than twice, every 90 days.
#in_fn = "{}_tm_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = 2
#cycle_length = 90

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "neutrals_hhs"

#3) Intenders: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than 3 times, every 30 days.
#in_fn = "{}_tm_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = 3
#cycle_length = 30

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "intenders_hhs"

#4) Weekly+ Single-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Single-Serve at least once every 7 days.
#in_fn = "{}_tm_single_serve_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = None
#cycle_length = 7

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "weekly_single_serve_hhs"

#5) Weekly+ Multi-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Multi-Serve at least once every 14 days.
#in_fn = "{}_tm_multi_serve_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = None
#cycle_length = 14

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "weekly_multi_serve_hhs"

#########################################################
###Simply
#########################################################

#brand = "simply"
#my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/{}_frequency/".format(brand)

#1) Rejectors: Over the last 52 weeks, have purchased Carbonated Soft Drinks at least twice every 30 days,
#but have not purchased Coca Cola TM.
#in_fn = "refrigerated_juices_no_{}_upcs.csv".format(brand)
#notin_fn = "{}_tm_upcs.csv".format(brand)

#max_weeks = 13
#freq_min = 2
#freq_max = None
#cycle_length = 30

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "rejectors_hhs"

#2) Neutrals: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than twice, every 90 days.
#in_fn = "{}_tm_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = 2
#cycle_length = 90

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "neutrals_hhs"

#3) Intenders: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than 3 times, every 30 days.
#in_fn = "{}_tm_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = 3
#cycle_length = 30

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "intenders_hhs"

#4) Weekly+ Single-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Single-Serve at least once every 7 days.
#in_fn = "{}_tm_single_serve_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = None
#cycle_length = 7

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "weekly_single_serve_hhs"

#5) Weekly+ Multi-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Multi-Serve at least once every 14 days.
#in_fn = "{}_tm_multi_serve_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = None
#cycle_length = 14

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "weekly_multi_serve_hhs"

#########################################################
###Smart Water
#########################################################

brand = "smart_water"
my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/{}_frequency/".format(brand)

#1) Rejectors: Over the last 52 weeks, have purchased Carbonated Soft Drinks at least twice every 30 days,
#but have not purchased Coca Cola TM.
#in_fn = "nf_water_no_{}_upcs.csv".format(brand)
#notin_fn = "{}_tm_upcs.csv".format(brand)

#max_weeks = 13
#freq_min = 2
#freq_max = None
#cycle_length = 30

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "rejectors_hhs"

#2) Neutrals: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than twice, every 90 days.
in_fn = "{}_tm_upcs.csv".format(brand)
notin_fn = None

max_weeks = 13
freq_min = 1
freq_max = 2
cycle_length = 90

output_fn = brand + "_" + "{}week_".format(max_weeks) + "neutrals_hhs"

#3) Intenders: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than 3 times, every 30 days.
#in_fn = "{}_tm_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = 3
#cycle_length = 30

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "intenders_hhs"

#4) Weekly+ Single-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Single-Serve at least once every 7 days.
#in_fn = "{}_tm_single_serve_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = None
#cycle_length = 7

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "weekly_single_serve_hhs"

#5) Weekly+ Multi-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Multi-Serve at least once every 14 days.
#in_fn = "{}_tm_multi_serve_upcs.csv".format(brand)
#notin_fn = None

#max_weeks = 13
#freq_min = 1
#freq_max = None
#cycle_length = 14

#output_fn = brand + "_" + "{}week_".format(max_weeks) + "weekly_multi_serve_hhs"

# COMMAND ----------


#Assumed schema for provided UPC lists
schema = t.StructType([
    t.StructField("Scan Upc", t.StringType(), True),
    t.StructField("Subcommodity Desc", t.StringType(), True),
    t.StructField("Commodity Desc", t.StringType(), True)
])
#Bought from the below UPCs every cycle
in_fp = my_dir + in_fn
in_upcs = spark.read.csv(in_fp, schema=schema)
in_upcs = in_upcs.select("Scan UPC").collect()
in_upcs = [x["Scan UPC"] for x in in_upcs]
#But never bought any of the below UPCs
if not (notin_fn is None):
  notin_fp = my_dir + notin_fn
  notin_upcs = spark.read.csv(notin_fp, schema=schema)
  notin_upcs = notin_upcs.select("Scan UPC").collect()
  notin_upcs = [x["Scan UPC"] for x in notin_upcs]
else:
  notin_upcs = []

#How many cycles are there. Partial cycles do not count!
num_cycles = int(m.floor((max_weeks*7)/cycle_length))

#Build a dataframe the specifies the cycle each day belongs to
days = list(range(1, (num_cycles*cycle_length)+1))
cycles = list(range(1, num_cycles+1)) * cycle_length
assert len(days) == len(cycles)
days.sort()
cycles.sort()
data = {
  'day': days,
  'cycle': cycles
}
cycles = pd.DataFrame(data)
cycles = spark.createDataFrame(cycles)
cycles.show(50, truncate=False)

# COMMAND ----------

import resources.config as config

#Pull in PIM to QC inputted lists
pim_fp = config.get_latest_modified_directory(config.azure_pim_core_by_cycle)
pim = config.spark.read.parquet(pim_fp)
pim = pim.select(
  f.col("upc").alias("gtin_no"),
  f.col("gtinName").alias("product_name"),
  f.col("krogerOwnedEcommerceDescription").alias("product_description"),
  f.col("familyTree.commodity.name").alias("commodity"),
  f.col("familyTree.subCommodity.name").alias("sub_commodity"),
)
pim = pim.select(
  "gtin_no", "product_description",
  "commodity", "sub_commodity",
)

#When we QC against PIM, we see that there are non-soda UPCs
acceptable_commodities = [
  "SOFT DRINKS", "SINGLE SERVE BEVERAGE", "SPECIALTY SOFT DRINKS",
  "REFRGRATD JUICES/DRINKS", "RTD TEA/LEMONADE", "SHELF STABLE JUICE", "FROZEN JUICE",
  "NF NAT FOODS WATER", "WATER", "SPARKLING WATER", "WATER",
]
acceptable_upcs = pim.filter(f.col("commodity").isin(acceptable_commodities)).select("gtin_no").collect()
acceptable_upcs = [x["gtin_no"] for x in acceptable_upcs]
in_upcs = [x for x in in_upcs if x in acceptable_upcs]

#
temp = pim.filter(f.col("gtin_no").isin(in_upcs))
message = "\nPIM data for inclusion UPCs:\n"
print(message)
temp.show(50, truncate=False)
del(temp)

if len(notin_upcs) != 0:
  notin_upcs = [x for x in notin_upcs if x in acceptable_upcs]

  temp = pim.filter(f.col("gtin_no").isin(notin_upcs))
  message = "\nPIM data for exclusion UPCs:\n"
  print(message)
  temp.show(50, truncate=False)
  del(temp)

# COMMAND ----------

#Define the date range for the ACDS pull 
today = dt.date.today()
last_monday = today - dr.datetime.timedelta(days=today.weekday())
start_date = last_monday - dr.datetime.timedelta(weeks=max_weeks)
end_date = last_monday - dr.datetime.timedelta(days=1)
#Pull in ACDS
acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))

#Add in a day and week column in your acds pull to enable easy querying
acds = acds.\
withColumn('year', f.substring('trn_dt', 1, 4)).\
withColumn('month', f.substring('trn_dt', 5, 2)).\
withColumn('day', f.substring('trn_dt', 7, 2))
acds = acds.withColumn('date', f.concat(f.col('month'), f.lit("-"), f.col("day"), f.lit("-"), f.col("year")))
acds = acds.select(
  "ehhn", "transaction_code", "gtin_no",
  "trn_dt", f.to_date(f.col("date"), "MM-dd-yyyy").alias("transaction_date"),
  "net_spend_amt",
)
acds = acds.withColumn("end_date", f.lit(end_date))
acds = acds.select("ehhn", "gtin_no", "transaction_code", "trn_dt", "transaction_date", "end_date",
                   f.datediff(f.col("end_date"), f.col("transaction_date")).alias("day"),
                   "net_spend_amt")
acds = acds.withColumn("week", f.col("day")/7)
acds = acds.withColumn("week", f.ceil(f.col("week")))
acds = acds.select("ehhn", "transaction_code", "gtin_no", "day", "week", "net_spend_amt")

#Merge w/ given UPC list to shorten query later.
acds = acds.filter(f.col("gtin_no").isin(in_upcs + notin_upcs))
acds = acds.dropDuplicates()
acds.cache()

acds.show(25, truncate=False)

# COMMAND ----------

#Get transaction data for everybody whose ever bought from in-list
#and designate which cycle each day belongs to
freq = acds.\
  filter(f.col("gtin_no").isin(in_upcs)).\
  join(cycles, "day", "inner")
freq.cache()

#Is frequency defined at the unit, basket, or day level? Basket level
freq = freq.groupBy("ehhn", "cycle").agg(f.countDistinct("transaction_code").alias("frequency"))
#Apply minimum condition: have purchased at least x times
if not (freq_min is None) and (type(freq_min) == int):
  freq = freq.filter(f.col("frequency") >= freq_min)
  print("Applied minimum-per-cycle condition.")

#Apply maximum condition: have purchased not more than x times
if not (freq_max is None) and (type(freq_max) == int):
  freq = freq.filter(f.col("frequency") <= freq_max)
  print("Applied maximum-per-cycle condition.")

#For every household, get how many cycles they satisfied
satisfied = freq.groupby("ehhn").agg(f.countDistinct("cycle").alias("cycles_satisfied"))
#QC: Check how many households met the conditions or were close to meeting the conidtions
#for each cycle
counts_df = satisfied.\
  groupBy("cycles_satisfied").\
  count().\
  orderBy("cycles_satisfied", ascending=False)
counts_df = counts_df.withColumnRenamed("count", "hh_count")

#Apply have-not-purchased condition: satsified the frequency conditions and did not
#purchase any of the products in the given list
if not (notin_upcs is None) and (type(notin_upcs) == list) and not (notin_fn is None):
  #For every applicable household, identify every household who bought from the bad list
  have_purch = satisfied.\
    join(acds, "ehhn", "inner").\
    filter(f.col("gtin_no").isin(notin_upcs)).\
    select("ehhn").\
    dropDuplicates()
  have_purch = have_purch.withColumn("did_purchase", f.lit(1))

  #Only keep the applicable households that did not buy from the bad list
  satisfied = satisfied.\
    join(have_purch, "ehhn", "left").\
    filter(f.col("did_purchase").isNull()).\
    select("ehhn", "cycles_satisfied")

  #QC:
  havenot_counts_df = satisfied.\
    groupBy("cycles_satisfied").\
    count().\
    orderBy("cycles_satisfied", ascending=False)
  havenot_counts_df = havenot_counts_df.withColumnRenamed("count", "havenot_hh_count")
  counts_df = counts_df.join(havenot_counts_df, "cycles_satisfied", "left")
  counts_df = counts_df.orderBy("cycles_satisfied", ascending=False)

#Cache the data
satisfied.cache()
counts_df.cache()

message = (
  "Inputted parameters for {}\n".format(output_fn) +
  "--------------------\n" +
  "Max Weeks: {}\n".format(max_weeks) +
  "Frequency Minimum: {}\n".format(freq_min) +
  "Frequency Maximum: {}\n".format(freq_max) +
  "Cycle Length: {}\n".format(cycle_length) +
  "--------------------\n"
)
print(message)
del(message)

counts_df.show(50, truncate=False)

# COMMAND ----------

water_cat_bool = True
top50 = True

if water_cat_bool:
  #Define the date range for the ACDS pull 
  today = dt.date.today()
  last_monday = today - dr.datetime.timedelta(days=today.weekday())
  start_date = last_monday - dr.datetime.timedelta(weeks=max_weeks)
  end_date = last_monday - dr.datetime.timedelta(days=1)
  #Pull in ACDS
  acds = ACDS(use_sample_mart=False)
  acds = acds.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))
  acds = acds.select("ehhn", "gtin_no", "net_spend_amt")

  water_fn = "water_category.csv"
  #Assumed schema for provided UPC lists
  schema = t.StructType([
      t.StructField("Scan Upc", t.StringType(), True),
      t.StructField("Subcommodity Desc", t.StringType(), True),
      t.StructField("Commodity Desc", t.StringType(), True)
  ])
  #Water category used for calculating water spenders
  water_fp = my_dir + water_fn
  water_upcs = spark.read.csv(water_fp, schema=schema)
  water_upcs = water_upcs.select("Scan UPC").collect()
  water_upcs = [x["Scan UPC"] for x in water_upcs]

  #filter(f.col("gtin_no").isin(water_upcs)).\
  if water_cat_bool:
    #Filter on water category UPCs and calculate household spending
    water_spenders = acds.\
    filter(f.col("net_spend_amt") > 0).\
    filter(f.col("gtin_no").isin(water_upcs)).\
    groupby("ehhn").\
    agg(f.min("net_spend_amt").alias("dollars_spent"))

    #Calculate 50 percentile cut-off
    cutoff50 = water_spenders.approxQuantile("dollars_spent", [0.50], 0.0001)
    cutoff50 = cutoff50[0]

    #Heavy - keep top 50 of water spenders
    if top50:
      water_spenders = water_spenders.filter(f.col("dollars_spent") >= cutoff50)
      output_fn = brand + "_" + "{}week_".format(max_weeks) + "heavy_" +  "neutrals_hhs"
      message = "Applied heavy spending conditions."
    else:
    #Light - keep bottom 50 of water spenders
      water_spenders = water_spenders.filter(f.col("dollars_spent") < cutoff50)
      output_fn = brand + "_" + "{}week_".format(max_weeks) + "light_" + "neutrals_hhs"
      message = "Applied light spending conditions."
    
    #Apply the condition to household by cycles satisfied
    water_spenders = water_spenders.select("ehhn")
    satisfied = satisfied.join(water_spenders, "ehhn", "inner")

    print(message)


# COMMAND ----------

#Output cycle counts dataframe
fp = my_dir + output_fn + "_cycles_count" + ".csv"
write_out(counts_df, fp)
del(fp)

#Output raw household dataframe that contains each household and how many cycles they satisfy
fp = my_dir + "raw_" + output_fn
satisfied.write.mode("overwrite").format("delta").save(fp)
del(fp)

#Apply every-x-days condition: satisfied the frequency conditions every cycle
satisfied = satisfied.filter(f.col("cycles_satisfied") >= int((num_cycles-0)))
message = "{} households met the frequency conditions every cycle!".format(satisfied.count())
print(message)

#Output household dataframe that contains each household that satisfies the required number of cycles
fp = my_dir + output_fn
satisfied.write.mode("overwrite").format("delta").save(fp)
del(in_fn, notin_fn, max_weeks, freq_min, freq_max, cycle_length, output_fn, fp)


# COMMAND ----------

#Diet Coke
#Rejectors - 1329596
#Neutrals - 2246481
#Intenders - 399372
#Single Serve - 2094
#Multi Serve - 94344
#Altogether - 4049402

#Coke Zero
#Rejectors - 1422015
#Neutrals - 1947512
#Intenders - 260291
#Single Serve - 1306
#Multi Serve - 42976
#Altogether - 3663058

#Sprite
#Rejectors - 1053701
#Neutrals - 4374678
#Intenders - 351191
#Single Serve - 843
#Multi Serve - 39653
#Altogether - 5808538

#Fanta
#Rejectors - 1585005
#Neutrals - 1329302
#Intenders - 47445
#Single Serve - 61
#Multi Serve - 2877
#Altogether - 2963790

#Minute Maid
#Rejectors - 2972522
#Neutrals - 2421885
#Intenders - 227971
#Single Serve - 34
#Multi Serve - 44606
#Altogether - 5620035

#Simply
#Rejectors - 339949
#Neutrals - 4459539
#Intenders - 477825
#Single Serve - 163
#Multi Serve - 85373
#Altogether - 5338787

#Smart Water
#Rejectors - 58228
#Neutrals - 635375
#Intenders - 21029
#Single Serve - 143
#Multi Serve - 4052
#Altogether - 717952

#Smart Water (07/30/24)
#Rejectors - 59569
#Neutrals - 679468
#Intenders - 21281
#Single Serve - 110
#Multi Serve - 3819
#Heavy Neutrals - 144793
#Light Neutrals - 217928
#Altogether - 760318

# COMMAND ----------

#Code used to egress most of Cola audiences

import pyspark.sql.functions as f
import datetime as dt

df = df1.union(df2).union(df3).union(df4).union(df5)

brand = "minute_maid"
my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/{}_frequency/".format(brand)

dfs = []
segs = [
  "rejectors",
  "neutrals",
  "intenders",
  "weekly_single_serve",
  "weekly_multi_serve",
]
for seg in segs:
  my_fn = brand + "_13week_" + seg + "_hhs"
  fp = my_dir + my_fn

  #Keep only ehhn and segment
  df = spark.read.format("delta").load(fp)
  df = df.select("ehhn")
  df = df.withColumn("segment", f.lit("H"))
  df.show(10, False)

  count = df.count()
  seg = brand + "_" + seg
  print("{} household count: {}".format(seg, count))
    
  #Write out to the egress directory and write out in format that engineering expects
  egress_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/egress"
  today = dt.datetime.today().strftime('%Y%m%d')
  dest_fp = egress_dir + '/' + seg + '/' + seg + '_' + today
  #df.write.mode("overwrite").format("parquet").save(dest_fp)
  print("SUCCESS - Wrote out to {}!\n\n".format(dest_fp))

  dfs += [df]


# COMMAND ----------

df1 = dfs[0]
df1 = df1.select("ehhn")
df2 = dfs[1]
df2 = df2.select("ehhn")
df3 = dfs[2]
df3 = df3.select("ehhn")
df4 = dfs[3]
df4 = df4.select("ehhn")
df5 = dfs[4]
df5 = df5.select("ehhn")

df = df1.union(df2).union(df3).union(df4).union(df5)
df = df.dropDuplicates()
df.count()

# COMMAND ----------

#Code used to egress over smart water audiences

max_weeks = 13
brand = "smart_water"
my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/{}_frequency/".format(brand)
my_fns = [
  brand + "_{}week_".format(max_weeks) + "rejectors_hhs",
  brand + "_{}week_".format(max_weeks) + "neutrals_hhs",
  brand + "_{}week_".format(max_weeks) + "intenders_hhs",
  brand + "_{}week_".format(max_weeks) + "heavy_" + "neutrals_hhs",
  brand + "_{}week_".format(max_weeks) + "light_" + "neutrals_hhs",
]
segments = [
  "rejectors",
  "neutrals",
  "intenders",
  "heavy_neutrals",
  "light_neutrals",
]

for my_fn, seg in zip(my_fns, segments):
  fp = my_dir + my_fn

  #Keep only ehhn and segment
  df = spark.read.format("delta").load(fp)
  df = df.select("ehhn")
  df = df.withColumn("segment", f.lit("H"))
  df.show(10, False)

  if seg == "intenders": 
    add_fn = brand + "_{}week_".format(max_weeks) + "weekly_single_serve_hhs"
    add_fp = my_dir + add_fn
    add_df = spark.read.format("delta").load(add_fp)
    df = df.union(add_df)
    df = df.withColumn("segment", f.lit("H"))
    df = df.dropDuplicates()

  count = df.count()
  seg = brand + "_" + seg
  print("{} household count: {}".format(seg, count))

  #Write out to the egress directory and write out in format that engineering expects
  egress_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/egress"
  today = dt.datetime.today().strftime('%Y%m%d')
  dest_fp = egress_dir + '/' + seg + '/' + seg + '_' + today
  #df.write.mode("overwrite").format("parquet").save(dest_fp)
  print("SUCCESS - Wrote out to {}!\n\n".format(dest_fp))
