# Databricks notebook source
"""
Creates sets of households by pulling in ACDS for 52 weeks of
transaction data and PIM for product hierarchy info. The
household sets are built as follows:

  1) From ACDS, keep any household that has purchased anything at
  any Kroger store in the past 52 weeks.

  2) For the given audience and department designation, keep only
  households who bought from the specified departments.

No propensity scoring, clustering, or any grouping is done on the
outputted household sets. The sales team only needs households that
simply purchased under the given department. The value of these 
households sets is for user utility and making Prism easier to use from
a time saving perspective.
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


import resources.config as config
import pyspark.sql.functions as f
from datetime import datetime, timedelta, date
from effodata import ACDS, golden_rules, joiner, sifter, join_on

import pyspark.sql.types as t
import commodity_segmentations.config as con

# COMMAND ----------

dept_dict = {
  "deli_and_bakery": ["DELI/BAKE"],
  "meat": ["MEAT"],
  "produce": ["PRODUCE"],
  "grocery": ["GROCERY"],
  "food": ["DELI/BAKE", "MEAT", "PRODUCE", "GROCERY"],
}

pim_fp = config.get_latest_modified_directory(config.azure_pim_core_by_cycle)
pim = config.spark.read.parquet(pim_fp)
pim = pim.select(
  f.col("upc").alias("gtin_no"),
  f.col("gtinName").alias("product_name"),
  f.col("krogerOwnedEcommerceDescription").alias("product_description"),
  f.col("familyTree.commodity.name").alias("commodity"),
  f.col("familyTree.subCommodity.name").alias("sub_commodity"),
  f.col("familyTree.primaryDepartment.name").alias("department"),
  f.col("familyTree.primaryDepartment.recapDepartment.name").alias("sub_department"),
  f.col("familyTree.primaryDepartment.recapDepartment.department.name").alias("micro_department"),
)
pim = pim.select(
  "gtin_no", "product_name", "product_description",
  "department", "sub_department", "micro_department",
  "commodity", "sub_commodity",
)
pim.cache()

# COMMAND ----------

#Pull 52 weeks of latest ACDS data - keep only gtin_no and ehhn

#Define the time range of data that you are pulling
max_weeks = 52
today = date.today()
last_monday = today - timedelta(days=today.weekday())
start_date = last_monday - timedelta(weeks=max_weeks)
end_date = last_monday - timedelta(days=1)

#Pull in transaction data
acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))
#Add in a week column in your acds pull to enable easy querying of variable weeks
acds = acds.select("ehhn", "gtin_no")
acds.cache()

# COMMAND ----------

#Format timestamp for when we write out UPC and household files
today = date.today().strftime('%Y%m%d')

for aud in list(dept_dict.keys()):
  #Pull relevant departments
  depts = dept_dict[aud]

  #Isolate the UPCs to the desired departments
  depts_upcs = pim.filter(f.col("department").isin(depts))
  depts_upcs = depts_upcs.select("gtin_no").dropDuplicates()

  message = f"{aud} UPC count: {depts_upcs.count()}"
  print(message)

  #Write out the UPC file
  fp = f'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/upc_lists/{aud}/{aud}_{today}'
  depts_upcs.write.mode("overwrite").format("delta").save(fp)
  print(f"Successfully wrote out {fp}!")
  del(message, fp)

  #Bump department UPCs against ACDS and keep only unique households
  depts_ehhns = acds.join(depts_upcs, "gtin_no", "inner")
  depts_ehhns = depts_ehhns.select("ehhn").dropDuplicates()
  depts_ehhns = depts_ehhns.withColumn("segment", f.lit("H"))

  message = f"{aud} household count: {depts_ehhns.count()}"
  print(message)

  #Write out the household file
  fp = f'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/department_segmentations/{aud}/{aud}_{today}'
  depts_ehhns.write.mode("overwrite").format("delta").save(fp)
  print(f"Successfully wrote out {fp}!")
  del(message, fp)


# COMMAND ----------

#Ideas for highlest level of department hierarchy
pim.select("department").dropDuplicates().collect()

# COMMAND ----------

#Ideas for second-highlest level of department hierarchy
pim.select("sub_department").dropDuplicates().collect()

# COMMAND ----------

#Ideas for lowest level of department hierarchy
pim.select("micro_department").dropDuplicates().collect()
