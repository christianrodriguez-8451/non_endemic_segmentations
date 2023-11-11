# Databricks notebook source
"""
Pulls in the most recent year of fuel transaction data via ACDS and
appends to it the PID file from the latest cycle to get sub-commodity designation.
For each of the fuel types, the households are ranked L, M, or H depending on how
many gallons of fuel they purchased. If the household purchased < the 33rd percentile,
then they are assigned to L. If the household purchased > 66th percentile, then they
are assigned to H. Otherwise, they are assigned M.

We repeat a similar methodology with the all-inclusive fuel segmentation. The value of
all-inclusive fuel is that it also considers 3rd party fuel providers. We could not
consider 3rd party fuel in the fuel types segmentation because 3rd party fuel does
not distinguish between fuel types.
"""

# COMMAND ----------

#When you use a job to run your notebook, you will need the service principles
#You only need to define what storage accounts you are using

#Define service principals
service_credential = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-pw')
service_application_id = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-app-id')
directory_id = "5f9dc6bd-f38a-454a-864c-c803691193c5"
storage_account = 'sa8451dbxadhocprd'
#storage_account = 'sa8451posprd'

#Set configurations
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

## Loading External Packages
import pyspark.sql.functions as f
import pyspark.sql.types as t
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window

## Loading Internal Packages
import config as con
from effodata import ACDS, golden_rules, joiner, sifter, join_on

# COMMAND ----------

#Pull one year of transaction data
end_date   = date.today().strftime("%Y-%m-%d") 
start_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=365)).strftime('%Y-%m-%d')
acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(
  start_date = start_date,
  end_date = end_date,
  apply_golden_rules=golden_rules('customer_exclusions'),
)

#Keep only fuel transactions and turn gallons into float
fuel = acds.filter(f.col('wgt_fl') == '3')
fuel = fuel.select( \
  f.col('ehhn'), \
  f.col('gtin_no'), \
  f.col('transactions.gtin_uom_am').alias('gallons'), \
)
fuel = fuel.withColumn("gallons", f.col("gallons").cast("float"))
fuel = fuel.na.drop(subset=["gallons"])

#Bump ACDS against PID to get commmodity + sub-commodity information
pid_path = "abfss://acds@sa8451posprd.dfs.core.windows.net/product/current"
pid = spark.read.parquet(pid_path)
pid = pid.select("gtin_no", "commodity_desc", "sub_commodity_desc", "con_dsc_tx")
pid = pid.dropDuplicates(["gtin_no"])
fuel = fuel.join(pid, on="gtin_no", how="inner")
#Third Party Fuel does not distinguish by fuel type (has no descriptive info from PID)
fuel = fuel.withColumn("sub_commodity_desc",
                       f.when(f.col("sub_commodity_desc") == "NO SUBCOMMODITY DESCRIPTION", "GASOLINE-THIRD PARTY")
                       .otherwise(f.col("sub_commodity_desc"))
)

print("Fuel Data Sample:\n")
fuel.show(25, truncate=False)

# COMMAND ----------

#Create a copy to do a propensity split regardless of gas type
#Third Party Fuel does not tell us fuel type - considering them altogether
#is an attempt to get value out of our third party fuel data
allfuel = fuel.alias("allfuel")
allfuel = allfuel.withColumn("sub_commodity_desc", f.lit("GASOLINE"))
data = fuel.union(allfuel)
data.cache()

cyc_date = date.today().strftime('%Y-%m-%d')
#Below is the line I use to hot fix a specific cycle
#cyc_date = "2023-11-03"
cyc_date = "cycle_date={}".format(cyc_date)
output_dir = con.output_fp + cyc_date
#For each gas type (and also all of them altogether), split the households into L, M, and H propensities
gas_types = ["GASOLINE", "GASOLINE-PREMIUM UNLEADED", "GASOLINE-REG UNLEADED", "GASOLINE-UNLEADED PLUS", "GASOLINE-THIRD PARTY"]
for gas_type in gas_types:

  #Aggregate total gallons bought for each houesholds
  df = data.filter(f.col("sub_commodity_desc") == gas_type)
  df = df.groupBy("ehhn").agg(f.sum("gallons").alias("gallons"))
  print(f"Total Household Count for {gas_type}: {df.count()}")

  #Assign each houeshold to either L, M, or H depending on how much
  #gas they bought (cut-offs are the 33rd and 66th quantiles)
  #L := low, M := medium, H := High
  percentiles = df.approxQuantile("gallons", [0.33, 0.66], 0.00001)
  df = df.withColumn(
    "segment",
    f.when(f.col("gallons") < percentiles[0], "L")
        .otherwise(f.when(f.col("gallons") > percentiles[1], "H")
                  .otherwise("M"))
  )

  #To QC the methodology
  counts = df.groupBy("segment").count()
  avgs = df.groupBy("segment").agg(f.avg(f.col("gallons")).alias("average_gallons"))
  print(f"{gas_type} Counts:\n")
  counts.show()
  print(f"{gas_type} Averages:\n")
  avgs.show()

  #Write-out
  if gas_type not in ["GASOLINE-THIRD PARTY"]:
    fn = gas_type.replace("-", "_")
    fn = fn.replace(" ", "_")
    fn = fn.lower()
    output_fp =  output_dir + "/" + fn
    df.write.mode("overwrite").format("delta").save(output_fp)
