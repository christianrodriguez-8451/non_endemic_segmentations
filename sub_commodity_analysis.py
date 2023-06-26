# Databricks notebook source
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

#Extract the relevant PID cycles for 2022 (year of analysis)
stub_path = "abfss://acds@sa8451posprd.dfs.core.windows.net/product/by_cycle/product_dim_{}/"
stubs_path = "abfss://acds@sa8451posprd.dfs.core.windows.net/product/by_cycle/"
cycles = dbutils.fs.ls(stubs_path)
cycles = list(cycles)
cycles = [c[0] for c in cycles]
cycles = [c.split("dim_")[1] for c in cycles]
cycles = [c.strip("/") for c in cycles]
cycles = [c for c in cycles if c[0:4] == "2022"]

#Empty container to catch dataframes as we iterate by cycle
schema = StructType([
  StructField('commodity_desc', StringType(), True),
  StructField('sub_commodity_desc', StringType(), True),
  StructField('ehhn', IntegerType(), True),
  StructField('cycle', StringType(), True),
  ])
subcomm_hh = spark.createDataFrame([], schema)
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

  #Message to keep track what cycle we are on
  message = "{}th cycle is from {} to {}!".format(i, prior, now)
  print(message)

  #Read in the correct PID cycle for the given time frame of the year
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
  dup_cols = ["commodity_desc", "sub_commodity_desc", "ehhn"]
  hh = hh.dropDuplicates(dup_cols)
  hh = hh.select(dup_cols)
  hh = hh.withColumn("cycle", lit(prior))
  subcomm_hh = subcomm_hh.union(hh)
  del(pid, acds, hh, dup_cols, prior)

subcomm_hh.cache()
subcomm_hh.show(25)

# COMMAND ----------

#In the past year, get the unique household count per sub-commodity
hh_counts = subcomm_hh.\
  groupBy(["commodity_desc", "sub_commodity_desc"]).\
  agg(countDistinct("ehhn").alias("households_count"))
hh_counts.cache()
hh_counts.is_cached

# COMMAND ----------

#~5900 sub-commodities
hh_counts.count()

# COMMAND ----------

#Write out your household counts
hh_counts.write.csv(output_fp)

# COMMAND ----------

#Install a package
%pip install wordcloud

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, lit, collect_list, split, substring, explode

stub_path = "abfss://acds@sa8451posprd.dfs.core.windows.net/product/current/"
pid = spark.read.parquet(stub_path)
pid = pid.select("sub_commodity_desc", "con_dsc_tx")

#Get the complete word occurence in each sub-commodity
word_count = pid.select("sub_commodity_desc", explode(split(pid["con_dsc_tx"], " ")).alias("word"))
word_count = word_count.groupBy(["sub_commodity_desc", "word"]).count()
word_count = word_count.sort(["sub_commodity_desc", "count"], ascending=False)

#Get the top n words in each sub-commodity
subcomm_windows = Window.partitionBy("sub_commodity_desc").orderBy(col("count").desc())
n = 100
top_n = word_count.withColumn("row", row_number().over(subcomm_windows)).filter(col("row") <= n)
top_n = top_n.groupBy('sub_commodity_desc').agg(collect_list('word').alias('top_words'))

#Get a sample of s products for each sub-commodity
pid = pid.withColumn("dummy", lit(1))
product_windows = Window.partitionBy("sub_commodity_desc").orderBy(col("dummy"))
s = 20
product_samples = pid.withColumn("row", row_number().over(product_windows)).filter(col("row") <= s)
product_samples = product_samples.groupBy('sub_commodity_desc').agg(collect_list('con_dsc_tx').alias('product_samples'))
product_samples.show(50)

# COMMAND ----------

hh_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/Users/c127414/sub_commodity_hh_count.csv"
hh_counts = spark.read.csv(hh_path, header=False)
hh_counts = hh_counts.toDF("commodity", "sub_commodity_desc", "hh_counts")

#Merge top words and the product sample against the sub-commodity household counts
desc_data = top_n.join(product_samples, on="sub_commodity_desc", how="inner")
hh_counts = hh_counts.join(desc_data, on="sub_commodity_desc", how="left")
hh_counts = hh_counts.withColumnRenamed("sub_commodity_desc", "sub_commodity")
hh_counts = hh_counts.withColumnRenamed("hh_counts", "household_counts")
hh_counts = hh_counts.select("commodity", "sub_commodity", "household_counts", "top_words", "product_samples")
hh_counts = hh_counts.sort(["commodity", "sub_commodity"])

#Need to turn list-columns into string columns
from pyspark.sql.functions import concat_ws
hh_counts = hh_counts.withColumn("top_words", concat_ws(", ", "top_words"))
hh_counts = hh_counts.withColumn("product_samples", concat_ws(", ", "product_samples"))
hh_counts.show(50)

# COMMAND ----------

save_location= "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/Users/c127414/"
csv_location = save_location+"temp.folder"
file_location = save_location+'sub_commodity_data.pipe'

hh_counts.repartition(1).write.csv(path=csv_location, mode="overwrite", header=True, sep="|")

file = dbutils.fs.ls(csv_location)[-1].path
dbutils.fs.cp(file, file_location)
dbutils.fs.rm(csv_location, recurse=True)

