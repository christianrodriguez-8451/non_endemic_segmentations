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
  "ASEPTIC JUICE", "FROZEN NOVELTIES-WATER ICE",
  "NF NAT FOODS WATER", "SPARKLING WATER", "WATER",
]
acceptable_upcs = pim.filter(f.col("commodity").isin(acceptable_commodities)).select("gtin_no").collect()
acceptable_upcs = [x["gtin_no"] for x in acceptable_upcs]

#Go through the TM list for each brand
schema = t.StructType([
    t.StructField("Scan Upc", t.StringType(), True),
    t.StructField("Subcommodity Desc", t.StringType(), True),
    t.StructField("Commodity Desc", t.StringType(), True)
])
my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/{}_frequency/"
in_fn = "{}_tm_upcs.csv"
brands = ["coca_cola", "diet_coke", "coke_zero", "sprite", "fanta", "minute_maid", "smart_water", "simply"]
my_upcs = []
in_upcs = []
for brand in brands:

  fp = my_dir.format(brand) + in_fn.format(brand)
  df = spark.read.csv(fp, schema=schema)
  upcs = df.select("Scan UPC").collect()
  upcs = [x["Scan UPC"] for x in upcs]
  upcs = [x for x in upcs if x in acceptable_upcs]

  my_upcs += [[upcs, brand]]
  in_upcs += upcs

# COMMAND ----------

max_weeks = 52

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
  "net_spend_amt", "scn_unt_qy",
)
acds = acds.withColumn("end_date", f.lit(end_date))
acds = acds.select("ehhn", "gtin_no", "transaction_code", "trn_dt", "transaction_date", "end_date",
                   f.datediff(f.col("end_date"), f.col("transaction_date")).alias("day"),
                   "net_spend_amt", "scn_unt_qy")
acds = acds.withColumn("week", f.col("day")/7)
acds = acds.withColumn("week", f.ceil(f.col("week")))
acds = acds.select("ehhn", "transaction_code", "gtin_no", "net_spend_amt", "scn_unt_qy", "day", "week")

#
acds = acds.withColumn('week', ((f.col('week') - 52) * -1) + 1)

#Merge w/ given UPC list to shorten query later.
acds = acds.filter(f.col("gtin_no").isin(in_upcs))
acds = acds.orderBy(["week"])
acds.cache()

acds.show(25, truncate=False)

# COMMAND ----------

weeks = acds.select("week").dropDuplicates()

for i in my_upcs:
  upcs = i[0]
  brand = i[1]

  temp = acds.filter(f.col("gtin_no").isin(upcs))
  temp = temp.\
    groupby("week").\
    agg(f.sum("net_spend_amt").alias("{}_net_spending".format(brand)),
        f.sum("scn_unt_qy").alias("{}_units_sold".format(brand)),
        f.countDistinct('ehhn').alias('{}_unique_households'.format(brand))
        )
  weeks = weeks.join(temp, "week", "left")

weeks.show(50, truncate=False)

# COMMAND ----------

fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + "52_week_coke_brands_analysis.csv"
write_out(weeks, fp)
