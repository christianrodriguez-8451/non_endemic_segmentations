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
import pyspark.sql.types as t
import datetime as dt
import os

def write_out(df, fp, delim=",", fmt="csv"):
  #Placeholder filepath for intermediate processing
  temp_target = os.path.dirname(fp) + "/" + "temp"

  #Write out df as partitioned file. Write out ^ delimited
  df.coalesce(1).write.options(header=True, delimiter=delim).mode("overwrite").format(fmt).save(temp_target)

  #Copy desired file from parititioned directory to desired location
  temporary_fp = os.path.join(temp_target, dbutils.fs.ls(temp_target)[3][1])
  dbutils.fs.cp(temporary_fp, fp)
  dbutils.fs.rm(temp_target, recurse=True)

#Get all productionized segmentations
segs = con.segmentations.all_segmentations
segs.sort()
problem_segs = []
egressed = []
fps = []
skipped = []
for seg in segs:
  try:
    #Get the filepath of the segmentation's latest file
    #We assume files are sorted (hence we use the last one) and they have a YYYYMMDD timestamp
    segment = con.segmentation(seg)
    fn = segment.files[-1]
    fp = segment.directory + fn
    print("Read in {} to egress.".format(fp))

    #Keep only ehhn and segment
    df = spark.read.format("delta").load(fp)
    df = df.select("ehhn", "segment")
  except:
    skipped += [seg]
    #Couldn't read file
    message = "Read-in failed for {}. Skipping egress.".format(seg)
    print(message)
    continue

  #Get count of all propensities in the file
  print("{} grouped-by household count:\n".format(seg))
  grouped_df = df.groupBy("segment").count()
  print(grouped_df.show(50, truncate=False))
  
  #Get count of propensities used in production
  filtered_df = df.filter(f.col("segment").isin(segment.propensities))
  filtered_count = filtered_df.count()
  print("{} household count: {}".format(seg, filtered_count))
  #If that count is too low, void egressing the file and throw an error at the end
  threshold = 50000
  if filtered_count < threshold:
    problem_segs += [seg]
    print("WARNING - Added {} to problematic segmentations.\n\n".format(seg))
    continue
    
  #Write out to the egress directory and write out in format that engineering expects
  egress_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/egress"
  today = dt.datetime.today().strftime('%Y%m%d')
  dest_fp = egress_dir + '/' + seg + '/' + seg + '_' + today
  #df.write.mode("overwrite").format("parquet").save(dest_fp)
  print("SUCCESS - Wrote out to {}!\n\n".format(dest_fp))

  #Keep track of file used in production
  egressed += [seg]
  fps += [fp]

#Write out the receipt that contains which was used for the given cycle
if len(fps) > 0:
  schema = t.StructType([
    t.StructField("audience", t.StringType(), True),
    t.StructField("filepath", t.StringType(), True)
  ])
  receipt_df = spark.createDataFrame(list(zip(egressed, fps)), schema=schema)
  receipt_fp = (
    "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/egress_receipts/" +
   "egress_{}.csv".format(today)
   )
  write_out(receipt_df, receipt_fp)

#Spit out error for the voided audiences
message = ""
#Throw an error for all the audiences that did not meet the threshold
if len(problem_segs) > 0:
  problem_segs.sort()
  addon = "There are {} problematic segmentations! Their household counts are far too low below the {} threshold. The problematic segmentations are: {}.\n".format(len(problem_segs), threshold, ", ".join(problem_segs))
  message += addon

#Throw an error for all the audiences that could not be read-in
if len(skipped) > 0:
  skipped.sort()
  addon = (
    "{} audiences were skipped due to read-in errors. ".format(len(skipped)) +
  "Those audiences are: {}.\n".format(", ".join(skipped))
  )
  message += addon

if len(message) > 0:
  raise ValueError(message)
