# Databricks notebook source
"""Pulls all transactions for the given year and gets the complete
household set at the commodity and sub-commodity level. With the household sets,
we proceed to calculate household counts for every commodity and sub-commodity.
At both the commodity and sub-commodity level, we extract the top words and
a sample of the products to have descriptive information on the given
commodity or sub-commodity. The final output are two pipe delimited files that
contain household counts, top words, and product sample at the commodity and
sub-commodity level.

To Do:
  Write out the final output as a single excel workbook instead of two
  pipe delimited files. Doing so will streamline the process.

  For the product samples column, consider using PIM instead of PID to
  get around the 20 character limit for product names.

  In the top words column, account for stop words! Think words that are
  most common in the complete product level (for the commodity level) and
  in the commodity level (for the sub-commodity level) as well.
"""

# COMMAND ----------

#Define service principals
service_credential = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-pw')
service_application_id = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-app-id')
directory_id = "5f9dc6bd-f38a-454a-864c-c803691193c5"
#storage_account = 'sa8451entlakegrnprd'
storage_account = 'sa8451dbxadhocprd'
#storage_account = 'sa8451posprd'

#Set configurations
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

from datetime import datetime
from dateutil.relativedelta import relativedelta
from effodata import ACDS, golden_rules, Joiner, Sifter, Equality, sifter, join_on, joiner
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import countDistinct, lit, sum, max

year = "2022"
#Extract the relevant PID cycles for 2022 (year of analysis)
stubs_path = "abfss://acds@sa8451posprd.dfs.core.windows.net/product/by_cycle/"
cycles = dbutils.fs.ls(stubs_path)
cycles = list(cycles)
cycles = [c[0] for c in cycles]
cycles = [c.split("dim_")[1] for c in cycles]
cycles = [c.strip("/") for c in cycles]
cycles = [c for c in cycles if c[0:4] == year]

#Empty container to catch dataframes as we iterate by cycle
#Todo: Create an additional schema and empty container for sub-commodities
schema_c = StructType([
  StructField('commodity_desc', StringType(), True),
  StructField('ehhn', IntegerType(), True),
  StructField('cycle', StringType(), True),
  ])
comm_hh = spark.createDataFrame([], schema_c)

schema_s = StructType([
  StructField('commodity_desc', StringType(), True),
  StructField('sub_commodity_desc', StringType(), True),
  StructField('ehhn', IntegerType(), True),
  StructField('cycle', StringType(), True),
  ])
subcomm_hh = spark.createDataFrame([], schema_s)
for i in range(0, len(cycles)):
  if i != (len(cycles) - 1):
    prior = cycles[i]
    now = cycles[i+1]
    now = datetime.fromisoformat("{}-{}-{}".format(now[0:4], now[4:6], now[6:8]))
    now = (now - relativedelta(days=1)).strftime('%Y%m%d')

  else:
    prior = cycles[i]
    now = cycles[i]
    now = datetime.fromisoformat("{}-{}-{}".format(now[0:4], now[4:6], now[6:8]))

  #Print message to keep track what cycle we are on
  message = "{}th cycle is from {} to {}!".format(i, prior, now)
  print(message)

  #Read in the correct PID cycle for the given time frame of the year
  #Todo: Keep sub-commodities as well
  stub_path = "abfss://acds@sa8451posprd.dfs.core.windows.net/product/by_cycle/product_dim_{}/"
  pid = spark.read.parquet(stub_path.format(prior))
  keep_cols = ["con_upc_no", "commodity_desc", "sub_commodity_desc"]
  pid = pid.select(keep_cols)
  del(message, keep_cols)

  #Read in relevant slice of ACDS to get transaction data
  prior = datetime.fromisoformat("{}-{}-{}".format(prior[0:4], prior[4:6], prior[6:8]))
  acds = ACDS(use_sample_mart=False)
  acds = acds.get_transactions(prior, now, apply_golden_rules=golden_rules(['customer_exclusions']))
  acds = acds.select("ehhn", "gtin_no")
  del(now)

  #Bump PID + ACDS to get unique households per sub-commodity and then add to our container
  pid = pid.withColumnRenamed("con_upc_no", "gtin_no")
  hh = acds.join(pid, on="gtin_no", how="inner")
  hh.cache()
  
  dup_cols = ["commodity_desc", "ehhn"]
  temp = hh.dropDuplicates(dup_cols)
  temp = temp.select(dup_cols)
  temp = temp.withColumn("cycle", lit(prior))
  comm_hh = comm_hh.union(temp)

  dup_cols = ["commodity_desc", "sub_commodity_desc", "ehhn"]
  temp = hh.dropDuplicates(dup_cols)
  temp = temp.select(dup_cols)
  temp = temp.withColumn("cycle", lit(prior))
  subcomm_hh = subcomm_hh.union(temp)
  
  del(pid, acds, hh, dup_cols, prior)

#In the past year, get the unique household count per commodity/sub-commodity
comm_hh_counts = comm_hh.\
  groupBy(["commodity_desc"]).\
  agg(countDistinct("ehhn").alias("households_count"))
comm_hh_counts = comm_hh_counts.withColumnRenamed("commodity_desc", "commodity")
comm_hh_counts.cache()

subcomm_hh_counts = subcomm_hh.\
  groupBy(["commodity_desc", "sub_commodity_desc"]).\
  agg(countDistinct("ehhn").alias("households_count"))
subcomm_hh_counts = subcomm_hh_counts.withColumnRenamed("commodity_desc", "commodity")
subcomm_hh_counts = subcomm_hh_counts.withColumnRenamed("sub_commodity_desc", "sub_commodity")
subcomm_hh_counts.cache()

comm_hh_counts.show(10)
subcomm_hh_counts.show(10)

# COMMAND ----------

#Get a quick and dirty count
#2022 - 834 commodities
#2022 - 5407 sub-commodities
print(comm_hh_counts.count())
print(subcomm_hh_counts.count())

# COMMAND ----------

import config as con
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, lit, collect_list, split, substring, explode, concat_ws
import pandas as pd

#Read in PID and only keep the necessary columns
stub_path = "abfss://acds@sa8451posprd.dfs.core.windows.net/product/current/"
pid = spark.read.parquet(stub_path)
pid = pid.select("commodity_desc", "sub_commodity_desc", "con_dsc_tx")
pid = pid.withColumnRenamed("commodity_desc", "commodity")
pid = pid.withColumnRenamed("sub_commodity_desc", "sub_commodity")
pid = pid.withColumn("dummy", lit(1))
#How many top words to pull from the product names
n = 100
#How many product samples to pull per commodity/sub-commodity
s = 20

output_fp = con.hh_counts_fp + "household_counts_2022.xlsx"
writer = pd.ExcelWriter(output_fp)
#For both commodities and sub-commodities
for comm in [["commodity", comm_hh_counts], ["sub_commodity", subcomm_hh_counts]]:
  column = comm[0]

  #Get the complete word occurence in each commodity/sub-commodity
  word_count = pid.select(column, explode(split(pid["con_dsc_tx"], " ")).alias("word"))
  word_count = word_count.groupBy([column, "word"]).count()
  word_count = word_count.sort([column, "count"], ascending=False)
  #Get the top n words in each commodity/sub-commodity
  comm_windows = Window.partitionBy(column).orderBy(col("count").desc())
  top_n = word_count.withColumn("row", row_number().over(comm_windows)).filter(col("row") <= n)
  top_n = top_n.groupBy(column).agg(collect_list('word').alias('top_words'))
  #Turn list-columns into string columns
  top_n = top_n.withColumn("top_words", concat_ws(", ", "top_words"))
  top_n = top_n.select(column, "top_words")

  #Get a sample of s products for each commodity/sub-commodity
  product_windows = Window.partitionBy(column).orderBy(col("dummy"))
  product_samples = pid.withColumn("row", row_number().over(product_windows)).filter(col("row") <= s)
  product_samples = product_samples.groupBy(column).agg(collect_list('con_dsc_tx').alias('product_samples'))
  #Turn list-columns into string columns
  product_samples = product_samples.withColumn("product_samples", concat_ws(", ", "product_samples"))
  product_samples = product_samples.select(column, "product_samples")

  #Merge top words and the product sample against the commodity/sub-commodity household counts
  desc_data = top_n.join(product_samples, on=column, how="inner")
  hh_counts = comm[1]
  hh_counts = hh_counts.join(desc_data, on=column, how="inner")

  hh_counts = hh_counts.to
  hh_counts.to_excel(writer, sheet_name=column)

  #Write out as a consolidate pipe delimited file
  #save_location= con.hh_counts_fp
  #csv_location = save_location+"temp.folder"
  #file_location = save_location + column + '_data_' + year + '.pipe'

  #hh_counts.repartition(1).write.csv(path=csv_location, mode="overwrite", header=True, sep="|")

  #my_file = dbutils.fs.ls(csv_location)[-1].path
  #dbutils.fs.cp(my_file, file_location)
  #dbutils.fs.rm(csv_location, recurse=True)


# COMMAND ----------


