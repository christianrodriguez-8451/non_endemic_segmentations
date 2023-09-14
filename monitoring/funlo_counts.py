# Databricks notebook source
"""
Reads in the latest segmentation file (outputted by products_embedding/weekly_household_behavior.py)
for each segmentation and gets a count of how many households are classified as L, M, H, new,
lapsed, and inconsistent. The final output is a csv file that has modality (ex. artsy_folk),
segmentation (ex. lapsed), and count (ex. 162).

To Do: Write out final file as an actual csv instead of a consolidated delta file.
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

#Get the directory for each segmentation and read in the latest file out there
seg_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/segment_behavior/segmentation/"
my_dirs = dbutils.fs.ls(seg_dir)
my_dirs = [x[0] for x in my_dirs if "modality" in x[1]]
my_fps = [get_latest_modified_directory(x) for x in my_dirs]

#Read in each segmentation and get the grouped-by counts for H, M, L, New, Lapsed, Inconsistent
schema = t.StructType([
  t.StructField('modality', t.StringType(), True),
  t.StructField('segmentation', t.StringType(), True),
  t.StructField('count', t.StringType(), True)
  ])
data = spark.createDataFrame([], schema)
for fp in my_fps:
  df = spark.read.format("delta").load(fp)
  df = df.groupBy(["modality", "segment"]).count()
  data = data.union(df)

data.head(5)

# COMMAND ----------

output_fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/commodity_segments/" + "funlo_hh_counts"
data.repartition(1).write.mode("overwrite").option("header", True).csv(output_fp)
