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

# COMMAND ----------

import config as con
import dateutil.relativedelta as dr
import datetime as dt
from effodata import ACDS, golden_rules
import pyspark.sql.functions as f
import pandas as pd

def get_latest_modified_directory(pDirectory):
    """
    get_latest_modified_file_from_directory:
        For a given path to a directory in the data lake, return the directory that was last modified. 
        Input path format expectation: 'abfss://x@sa8451x.dfs.core.windows.net/
    """
    #Set to get a list of all folders in this directory and the last modified date time of each
    vDirectoryContentsList = list(dbutils.fs.ls(pDirectory))

    #Convert the list returned from get_dir_content into a dataframe so we can manipulate the data easily. Provide it with column headings. 
    #You can alternatively sort the list by LastModifiedDateTime and get the top record as well. 
    df = spark.createDataFrame(vDirectoryContentsList,['FullFilePath', 'LastModifiedDateTime'])

    #Get the latest modified date time scalar value
    maxLatestModifiedDateTime = df.agg({"LastModifiedDateTime": "max"}).collect()[0][0]

    #Filter the data frame to the record with the latest modified date time value retrieved
    df_filtered = df.filter(df.LastModifiedDateTime == maxLatestModifiedDateTime)
    
    #return the file name that was last modifed in the given directory
    return df_filtered.first()['FullFilePath']


pim_path = get_latest_modified_directory("abfss://pim@sa8451posprd.dfs.core.windows.net/pim_core/by_cycle/")
pim_core = spark.read.parquet(pim_path)
cols = list(pim_core.columns)
pim_core = pim_core.select(
  "upc", "gtin", "gtinName", "krogerOwnedEcommerceDescription",
  f.col("familyTree.commodity.name").alias("commodity"),
  f.col("familyTree.subCommodity.name").alias("sub_commodity"),
)
for colname in pim_core.columns:
    pim_core = pim_core.withColumn(colname, f.trim(f.col(colname)))

pim_core = pim_core.filter(
  f.col("gtinName").like("%DIET%") | (f.col("krogerOwnedEcommerceDescription").like("%DIET%"))
)
pim_core.count()

# COMMAND ----------

bad_comms = [
  "DRINKWARE/FLATWARE", "PET CARE SUPPLIES", "TEAM LICENSED NON-APPAREL",
  "NARCOTICS/CNTRL SUB/LGND/NONLG", "MISCELLANEOUS TRANSACTIONS",
  "MISC SEASONAL&SOUVENIRS", "HAIR CARE PRODUCTS", "FOOD PREP",
  "WET CAT FOOD", "WET DOG FOOD", "VITAMIN PRODUCTS",
]
pim_core = pim_core.where(~f.col("commodity").isin(bad_comms))
pim_core.count()

# COMMAND ----------

#Define the time range of data that you are pulling
max_weeks = 52
today = dt.date.today()
last_monday = today - dr.datetime.timedelta(days=today.weekday())
start_date = last_monday - dr.datetime.timedelta(weeks=max_weeks)
end_date = last_monday - dr.datetime.timedelta(days=1)

#Pull in transaction data
acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))
acds = acds.select("gtin_no", "ehhn")
acds = acds.dropDuplicates()
acds = acds.join(pim_core, acds.gtin_no == pim_core.upc)
acds = acds.dropDuplicates(["ehhn"])
print("Diet segmentation's raw household count: {}".format(acds.count()))

# COMMAND ----------

pim_core = pim_core.withColumnRenamed("upc", "gtin_no")
pim_core = pim_core.select("gtin_no")
pim_core = pim_core.dropDuplicates()

#Write-out the file
cyc_date = dt.date.today().strftime('%Y-%m-%d')
cyc_date = "cycle_date={}".format(cyc_date)
output_dir = con.output_fp + cyc_date
if not (cyc_date in list(dbutils.fs.ls(con.output_fp))):
  dbutils.fs.mkdirs(output_dir)

output_fp =  output_dir + "/" + "dieting"
pim_core.write.mode("overwrite").format("delta").save(output_fp)

# COMMAND ----------


