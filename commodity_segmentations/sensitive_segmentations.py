# Databricks notebook source
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
from effodata import ACDS, golden_rules, joiner, sifter, join_on
from pyspark.sql import SparkSession
import resources.config as config
import config as con

spark = SparkSession.builder.appName("sensitive_segmentations").getOrCreate()

# COMMAND ----------


#Define the time range of data that you are pulling
max_weeks = 26
today = date.today()
last_monday = today - timedelta(days=today.weekday())
start_date = last_monday - timedelta(weeks=max_weeks)
end_date = last_monday - timedelta(days=1)
del(today)

today = date.today().strftime('%Y%m%d')

#Pull in transaction data
acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))
#Add in a week column in your acds pull to enable easy querying of variable weeks
acds = acds.\
withColumn('year', f.substring('trn_dt', 1, 4)).\
withColumn('month', f.substring('trn_dt', 5, 2)).\
withColumn('day', f.substring('trn_dt', 7, 2))
acds = acds.withColumn('date', f.concat(f.col('month'), f.lit("-"), f.col("day"), f.lit("-"), f.col("year")))
acds = acds.select("ehhn", "gtin_no", "trn_dt", f.to_date(f.col("date"), "MM-dd-yyyy").alias("transaction_date"))
acds = acds.withColumn("end_date", f.lit(end_date))
acds = acds.select("ehhn", "gtin_no", "trn_dt", "transaction_date", "end_date",
                   f.datediff(f.col("end_date"), f.col("transaction_date")).alias("day"))
acds = acds.withColumn("week", f.col("day")/7)
acds = acds.withColumn("week", f.ceil(f.col("week")))
acds = acds.select("ehhn", "gtin_no", "week")

#Pull upc hierarchy from PIM to assign sub-commodity to transactions
pim_fp = config.get_latest_modified_directory(config.azure_pim_core_by_cycle)
pim = config.spark.read.parquet(pim_fp)
pim = pim.select(
  f.col("upc").alias("gtin_no"),
  f.col("familyTree.commodity").alias("commodity"),
  f.col("familyTree.subCommodity").alias("sub_commodity"),
)
pim = pim.withColumn("commodity", f.col("commodity").getItem("name"))
pim = pim.withColumn("sub_commodity", f.col("sub_commodity").getItem("name"))
pim = pim.na.drop(subset=["sub_commodity"])
acds = acds.join(pim, "gtin_no", "inner")

#Filter for only the relevant commodities and sub-commodities to keep the dataframe small
segments_dict = con.sensitive_segmentations
my_comms = []
my_subs = []
for s in list(segments_dict.keys()):
  my_comms += segments_dict[s]["commodities"]
  my_subs += segments_dict[s]["sub_commodities"]

my_comms = list(set(my_comms))
my_subs = list(set(my_subs))
acds = acds.filter((acds["commodity"].isin(my_comms)) | (acds["sub_commodity"].isin(my_subs)))
del(my_comms, my_subs)

acds = acds.select("ehhn", "commodity", "sub_commodity", "week")
acds = acds.dropDuplicates()
acds.cache()

acds.show(50, truncate=False)

# COMMAND ----------

#Output the ehhn set for each segmentations
for s in list(segments_dict.keys()):
  #Un-pack the parameters for the given segment
  my_dict = segments_dict[s]
  comms = my_dict["commodities"]
  sub_comms = my_dict["sub_commodities"]
  n_weeks = my_dict["weeks"]

  #Filter only on the commodities and sub-commodities relevant to the given segment
  lil_acds = acds.filter((acds["commodity"].isin(comms)) | (acds["sub_commodity"].isin(sub_comms)))
  lil_acds = lil_acds.filter(lil_acds["week"] <= n_weeks)

  #Visually see commodities and sub-commodities used (QC)
  temp = lil_acds.select("commodity", "sub_commodity")
  temp = temp.orderBy(["commodity", "sub_commodity"])
  temp = temp.dropDuplicates()
  print(temp.show(50, truncate=False))

  #Get a count of how many distinct households (QC)
  ehhns = lil_acds.select("ehhn").dropDuplicates()
  message = ("{} household count: {}".format(s, ehhns.count()))
  print(message)

  #Output the distinct household set with all given "H" propensity
  ehhns = ehhns.withColumn("segment", f.lit("H"))
  fp = f'{con.output_fp}sensitive_segmentations/{s}/{s}_{today}'
  ehhns.write.mode("overwrite").format("delta").save(fp)

# COMMAND ----------


