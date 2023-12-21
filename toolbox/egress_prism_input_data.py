# Databricks notebook source
"""
Reads in the input data meant for PRISM and converts it from delta to parquet.
During the read-in, ehhn and segment are the only columns kept. The data is written
out to audience factory's egress directory.

To Do:

  1) Have this auto-execute for every deployed segmentation.
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

import toolbox.config as con
import pyspark.sql.functions as f
import datetime as dt

#Get all productionized segmentations
segs = con.segmentations.all_segmentations
segs.sort()
problem_segs = []
for seg in segs:
  #Get the filepath of the segmentation's latest file
  #We assume files are sorted (hence we use the last one) and they have a YYYYMMDD timestamp
  segment = con.segmentation(seg)
  fn = segment.files[-1]
  fp = segment.directory + fn
  print("Read in {} to egress.".format(fp))

  #Keep only ehhn and segment
  df = spark.read.format("delta").load(fp)
  df = df.select("ehhn", "segment")

  #Get count of all propensities in the file
  print("{} grouped-by household count:\n".format(seg))
  grouped_df = df.groupBy("segment").count()
  print(grouped_df.show(50, truncate=False))
  
  #Get count of propensities used in production
  filtered_df = df.filter(f.col("segment").isin(segment.propensities))
  filtered_count = filtered_df.count()
  print("{} household count: {}".format(seg, filtered_count))
  #If that count is too low, void egressing the file and throw an error at the end
  threshold = 1750000
  if filtered_count < threshold:
    problem_segs += [seg]
    print("WARNING - Added {} to problematic segmentations.\n\n".format(seg))
    continue
    
  #Write out to the egress directory and write out in format that engineering expects
  egress_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/egress"
  today = dt.datetime.today().strftime('%Y%m%d')
  dest_fp = egress_dir + '/' + seg + '/' + seg + '_' + today
  df.write.mode("overwrite").format("parquet").save(dest_fp)
  print("SUCCESS - Wrote out to {}!\n\n".format(dest_fp))

#Throw an error for all the segmentations that did not meet the threshold
if len(problem_segs) > 0:
  problem_segs.sort()
  message = "There are {} problematic segmentations! Their household counts are far too low below the {} threshold. The problematic segmentations are: {}.".format(len(problem_segs), threshold, ", ".join(problem_segs))
  raise ValueError(message)

# COMMAND ----------

#Code used to investigate the data issues
bad_segs = [
  'free_from_gluten', 'grain_free', 'healthy_eating',
  'ketogenic', 'kidney-friendly', 'lactose_free',
  'low_bacteria', 'low_fodmap', 'mediterranean_diet',
  'paleo', 'vegan', 'vegetarian',
]

for b in bad_segs:
  count = 0
  segment = con.segmentation(b)
  files = segment.files
  files.reverse()

  for i in files:
    fp = segment.directory + i
    df = spark.read.format("delta").load(fp)
    df = df.select("ehhn", "segment")

    filtered_df = df.filter(f.col("segment").isin(segment.propensities))
    filtered_count = filtered_df.count()
    if filtered_count >= 1750000:
      print("{}-{} has enough rows!".format(b, i))
      break

    else:
      print("{}-{} has {} records...".format(b, i, filtered_count))

  print("{} has no files with enough rows!".format(b))
