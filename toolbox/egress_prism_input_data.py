# Databricks notebook source
"""
Reads in the input data meant for PRISM and converts it from delta to parquet.
During the read-in, ehhn and segment are the only columns kept. The data is written
out to audience factory's egress directory.

To Do:

  1) Have this auto-execute for every deployed segmentation.

  2) Build API on top of this that asks the user the backend name,
  column for ehhn, column for columnName, and even smart filepathing.
  Once given the parameters, it auto loads to the deployed segmentations.
  How to keep track of the deployed? Python module? Class? JSON?
"""

# COMMAND ----------

#When you use a job to run your notebook, you will need the service principles
#You only need to define what storage accounts you are using

#Define service principals
service_credential = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-pw')
service_application_id = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-app-id')
directory_id = "5f9dc6bd-f38a-454a-864c-c803691193c5"
storage_account = 'sa8451dbxadhocprd'

#Set configurations
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window

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

seg_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/segment_behavior/segmentation/modality=lactose_free/"
fp = get_latest_modified_directory(seg_dir)
#stratum = spark.read.format("delta").load(fp)

#print(stratum.show(5))
#print(stratum.select("segment").distinct().collect())

# COMMAND ----------

#Took out heart_friendly since not found (10.12.2023)
from commodity_segmentations import percentile_segmentations
segmentations = {
  "funlo": [
    "free_from_gluten", "grain_free", "healthy_eating", "heart_friendly",
    "ketogenic", "kidney-friendly", "lactose_free", "low_bacteria", "paleo",
    "vegan", "vegetarian", "beveragist", "breakfast_buyers", "hispanic_cuisine",
    "low_fodmap", "mediterranean_diet", "organic", "salty_snackers",
    "non_veg", "low_salt", "low_protein", "heart_friendly", "macrobiotic",
    ],
  "percentile": percentile_segmentations,
  "fuel": ["gasoline", "gasoline_premium_unleaded", "gasoline_unleaded_plus", "gasoline_reg_unleaded"],
}
filepaths = {
  "funlo": "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/embedded_dimensions/customer_data_assets/segment_behavior/segmentation/modality={}/",
  "percentile": "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/percentile_segmentations/{}/",
  "fuel": "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/fuel_segmentations/{}/",
}

segs = segmentations["funlo"] + segmentations["percentile"] + segmentations["fuel"]
for seg in segs:
  #try:
    #Read in latest directory of segment
    if seg in segmentations["funlo"]:
      fp = filepaths["funlo"].format(seg)
      fp = get_latest_modified_directory(fp)

    elif seg in segmentations["fuel"]:
      fp = filepaths["fuel"].format(seg)
      fp = get_latest_modified_directory(fp)

    elif seg in segmentations["percentile"]:
      fp = filepaths["percentile"].format(seg)
      fp = get_latest_modified_directory(fp)

    
    print("Latest directory is {}!".format(fp))

    #Keep only ehhn and segment
    stratum = spark.read.format("delta").load(fp)
    stratum = stratum.select("ehhn", "segment")

    print("segmentation: {}".format(seg))
    print(stratum.show(10))
    print(stratum.count())

    #Write out to the egress directory and write out in format that engineering expects
    egress_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/egress"
    today = "20231120"
    dest_fp = egress_dir + '/' + seg + '/' + seg + '_' + today
    stratum.write.mode("overwrite").format("parquet").save(dest_fp)
    print("SUCCESS: Wrote out to {} successfully!".format(dest_fp))

  #except:
  #  print("ALERT: Could not find input data for {}!".format(seg))
