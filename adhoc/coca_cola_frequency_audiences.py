# Databricks notebook source
import dateutil.relativedelta as dr
import datetime as dt
from effodata import ACDS, golden_rules
import pyspark.sql.functions as f
import pyspark.sql.types as t
import pandas as pd
import math as m
from pyspark.sql.window import Window

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

my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/coca_cola_frequency/"

#1) Rejectors: Over the last 52 weeks, have purchased Carbonated Soft Drinks at least twice every 30 days,
#but have not purchased Coca Cola TM.
#in_fn = "carbonated_soft_drinks_upcs.csv"
#notin_fn = "coca_cola_tm_upcs.csv"

#max_weeks = 52
#freq_min = 2
#freq_max = None
#cycle_length = 30

#output_fn = "rejectors_hhs"

#2) Neutrals: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than twice, every 90 days.
#in_fn = "coca_cola_tm_upcs.csv"
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = 2
#cycle_length = 90

#output_fn = "neutrals_hhs"

#3) Intenders: Over the last 52 weeks, have purchased Coca Cola TM at least once,
#but not more than 3 times, every 30 days.
#in_fn = "coca_cola_tm_upcs.csv"
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = 3
#cycle_length = 30

#output_fn = "intenders_hhs"

#4) Weekly+ Single-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Single-Serve at least once every 7 days.
#in_fn = "coca_cola_tm_single_serve_upcs.csv"
#notin_fn = None

#max_weeks = 52
#freq_min = 1
#freq_max = None
#cycle_length = 7

#output_fn = "weekly_single_serve_hhs"

#5) Weekly+ Multi-Serve: Over the last 52 weeks, have purchased
#Coca Cola TM Multi-Serve at least once every 14 days.
in_fn = "coca_cola_tm_multi_serve_upcs.csv"
notin_fn = None

max_weeks = 52
freq_min = 1
freq_max = None
cycle_length = 14

output_fn = "weekly_multi_serve_hhs"

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
  #f.col("gtin").alias("gtin_no"),
  f.col("gtinName").alias("product_name"),
  f.col("krogerOwnedEcommerceDescription").alias("product_description"),
  f.col("familyTree.commodity.name").alias("commodity"),
  f.col("familyTree.subCommodity.name").alias("sub_commodity"),
)
pim = pim.select(
  "gtin_no", "product_description",
  "commodity", "sub_commodity",
)

#
temp = pim.filter(f.col("gtin_no").isin(in_upcs))
message = "\nPIM data for inclusion UPCs:\n"
print(message)
temp.show(50, truncate=False)
del(temp)

if len(notin_upcs) != 0:
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
  "trn_dt", f.to_date(f.col("date"), "MM-dd-yyyy").alias("transaction_date")
)
acds = acds.withColumn("end_date", f.lit(end_date))
acds = acds.select("ehhn", "gtin_no", "transaction_code", "trn_dt", "transaction_date", "end_date",
                   f.datediff(f.col("end_date"), f.col("transaction_date")).alias("day"))
acds = acds.withColumn("week", f.col("day")/7)
acds = acds.withColumn("week", f.ceil(f.col("week")))
acds = acds.select("ehhn", "transaction_code", "gtin_no", "day", "week")

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

satisfied = freq.groupby("ehhn").agg(f.countDistinct("cycle").alias("cycles_satisfied"))
#QC: Check how many households met the conditions or were close to meeting the conidtions
#for each cycle
counts_df = satisfied.\
  groupBy("cycles_satisfied").\
  count().\
  orderBy("cycles_satisfied", ascending=False)

windowSpec = Window.orderBy(f.desc("cycles_satisfied"))
counts_df = counts_df.withColumn("row_index", f.row_number().over(windowSpec))
counts_df = counts_df.filter(f.col("row_index") <= 10)
counts_df.show(50, truncate=False)

#Apply every-x-days condition: satisfied the frequency conditions every cycle
satisfied = satisfied.filter(f.col("cycles_satisfied") == num_cycles)
message = "{} households met the frequency conditions every cycle!".format(satisfied.count())
print(message)

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
    select("ehhn")

  satisfied.cache()

  message = "Out of all the applicable households, {} households never bought from the notin list.".format(satisfied.count())
  print(message)


# COMMAND ----------

#Write out the household file
output_fp = my_dir + output_fn
satisfied.write.mode("overwrite").format("delta").save(output_fp)
del(output_fn, output_fp)
