# Databricks notebook source
spark.conf.set("spark.sql.shuffle.partitions","auto")
from pyspark.sql.functions import collect_list
from pathlib import Path

# COMMAND ----------

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
#from pyspark.sql.functions import col, row_number, lit, collect_list, split, substring, explode, concat_ws
#from pyspark.sql.types import StructType, StructField, StringType, IntegerType
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


fp = get_latest_modified_directory("abfss://pim@sa8451posprd.dfs.core.windows.net/pim_core/by_cycle/")
pim = spark.read.parquet(fp)
cols = list(pim.columns)
pim = pim.select(
  "upc", "gtinName", "krogerOwnedEcommerceDescription",
)
for colname in pim.columns:
    pim = pim.withColumn(colname, f.trim(f.col(colname)))

pim = pim.dropDuplicates(["upc"])
pim = pim.withColumn("dummy", f.lit(1))

# COMMAND ----------

import commodity_segmentations.config as con
import pyspark.sql.functions as f
import pyspark.sql.types as t

#Collect all the UPCs for each segmentation

#Read in the UPCs from the commodity segmentation module
#Get all filepaths and segmentations made under commodity segmentations
output_dir = con.output_fp
output_dir = get_latest_modified_directory(output_dir)
contents = dbutils.fs.ls(output_dir)
filepaths = [x[0] for x in contents]
segments = [s[1] for s in contents]
segments = [s.strip("/") for s in segments]

#Read in all their UPC lists and concat together into one
schema = t.StructType([
  t.StructField('upc', t.StringType(), True),
  t.StructField('segmentation', t.StringType(), True),
  ])
upcs = spark.createDataFrame([], schema) 
for fp, seg in zip(filepaths, segments):
  seg_upcs = spark.read.format("delta").load(fp)
  seg_upcs = seg_upcs.select(f.col("gtin_no").alias("upc"))
  seg_upcs = seg_upcs.withColumn("segmentation", f.lit(seg))
  upcs = upcs.union(seg_upcs)

upcs.count()

# COMMAND ----------

output_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/diet_query_embeddings/cycle_date=2023-07-23"
contents = dbutils.fs.ls(output_dir)
filepaths = [x[0] for x in contents]
segments = [s[1] for s in contents]
segments = [s.strip("/") for s in segments]

for fp, seg in zip(filepaths, segments):
  seg_upcs = spark.read.format("delta").load(fp)
  seg_upcs = seg_upcs.select(f.col("gtin_no").alias("upc"))
  seg_upcs = seg_upcs.withColumn("segmentation", f.lit(seg))
  upcs = upcs.union(seg_upcs)

upcs.count()

# COMMAND ----------

#Merge the segmentation UPC list agains the PIM information
upcs = upcs.join(pim, on="upc", how="inner")
upcs = upcs.withColumnRenamed("gtinName", "product_name")
upcs = upcs.withColumnRenamed("krogerOwnedEcommerceDescription", "product_description")
upcs.show(25, truncate=False)

# COMMAND ----------

from pyspark.sql.window import Window

#How many top words to pull from the product names
n = 100
#How many product samples to pull per commodity/sub-commodity
s = 30
#For both product names and product descriptions
schema = t.StructType([
  t.StructField('segmentation', t.StringType(), True),
  t.StructField('source', t.StringType(), True),
  t.StructField('top_words', t.StringType(), True),
  t.StructField('samples', t.StringType(), True),
])
data = spark.createDataFrame([], schema)
for column in ["product_name", "product_description"]:
  temp = upcs.filter(
  (~(f.col(column).isNull())) &
  (~(upcs[column] == ""))
  )
  #Get rid of GTIN Name in product name
  if column == "gtinName":
    temp = temp.filter(
      (~(temp[column] == "GTIN Name"))
    )

  #Get the complete word occurence in each commodity/sub-commodity
  word_count = temp.select("segmentation", f.explode(f.split(temp[column], " ")).alias("word"))
  word_count = word_count.groupBy(["segmentation", "word"]).count()
  word_count = word_count.sort(["segmentation", "count"], ascending=False)
  #Get the top n words in each commodity/sub-commodity
  comm_windows = Window.partitionBy("segmentation").orderBy(f.col("count").desc())
  top_n = word_count.withColumn("row", f.row_number().over(comm_windows)).filter(f.col("row") <= n)
  top_n = top_n.groupBy("segmentation").agg(f.collect_list('word').alias('top_words'))
  top_n = top_n.withColumn("top_words", f.concat_ws(", ", "top_words"))
  top_n = top_n.select("segmentation", "top_words")

  #Get a sample of s products for each commodity/sub-commodity
  product_windows = Window.partitionBy("segmentation").orderBy(f.col("dummy"))
  product_samples = temp.withColumn("row", f.row_number().over(product_windows)).filter(f.col("row") <= s)
  product_samples = product_samples.groupBy("segmentation").agg(f.collect_list(column).alias('samples'))
  if column == "product_name":
    product_samples = product_samples.withColumn("samples", f.concat_ws(", ", "samples"))
  elif column == "product_description":
    product_samples = product_samples.withColumn("samples", f.concat_ws(".  ", "samples"))

  product_samples = product_samples.select("segmentation", "samples")

  #Merge top words and the product sample against the commodity/sub-commodity household counts
  temp = top_n.join(product_samples, on="segmentation", how="inner")
  temp = temp.withColumn("source", f.lit(column))
  temp = temp.select("segmentation", "source", "top_words", "samples")
  data = data.union(temp)

data = data.sort(["segmentation", "source"])
data.show(25)

# COMMAND ----------

#Way to output a dummy JSON file
import pandas as pd
import os

temp_target = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/commodity_segments/temp"
real_target = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/commodity_segments/" + "segmentations_words"

output_df = data
output_df.coalesce(1).write.options(header=True, delimiter="^").mode("overwrite").format('csv').save(temp_target)
temporary_fp = os.path.join(temp_target, dbutils.fs.ls(temp_target)[3][1])

print(temporary_fp)
dbutils.fs.cp(temporary_fp, real_target)
dbutils.fs.rm(temp_target, recurse=True)
