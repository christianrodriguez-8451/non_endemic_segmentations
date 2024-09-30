# Databricks notebook source
"""
Demonstrates how to isolate UPCs that have a valid image at
the Kroger website from the UPCs that not have a valid image.
This is critical to WNS efforts because there is a UPC carasoul
for each UPC-based audience on the Audience Monitoring dashboard.

This is achieved by first defining a function that takes in the
image's URL and calculates the image's size (width x height) or
returns a -1 if the image has no size to return (aka invalid image).
Next, we create a Pyspark UDF using that sizing function and apply
the UDF on each UPC present in PIM. Finally, we only keep UPCs that
have an image size > 0.
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

#Packages Used
import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t

import requests
import io
from PIL import Image

import resources.config as config
from effodata import ACDS, golden_rules, Joiner, Sifter, Equality, join_on 
from kpi_metrics import KPI, AliasMetric, CustomMetric, AliasGroupby

# COMMAND ----------

#Image sizing function
def get_image_size(url: str) -> int:
  """ Returns size of the image located at the inputted URL.

  Example
  ----------
    get_image_size('https://www.kroger.com/product/images/medium/front/0019264047717')

  Parameters
  ----------
  url: str
    URL string of where the image is located.

  Returns
  ----------
  A two-dimension tuple with the size image or None if there is no
  image at the inputted URL.
  """
  try:
    # Download the image from the URL
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for 4xx and 5xx status codes

    # Open the image using Pillow
    image_data = io.BytesIO(response.content)
    image = Image.open(image_data)

    # Get the size of the image
    width, height = image.size
    return (int(width*height))
  
  except Exception as e:
    return (-1)

#Read in PIM
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

#Create URL column for the size udf
url_base = "https://www.kroger.com/product/images/medium/front/"
pim = pim.withColumn("url", f.concat(f.lit(url_base), f.col("gtin_no")))
#Shove each URL through the size udf to get the size of each product image
#If the product has no size, then it has -1 for size (hence no image).
size_udf = f.udf(get_image_size, t.IntegerType())
pim = pim.withColumn("url_size", size_udf(f.col("url")))
#Keep only UPCs that had a valid image we could get sizing for
pim = pim.filter(f.col("url_size") > 0)
pim = pim.select("gtin_no", "url")

#Write-out the file
output_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/powerbi_inputs/"
output_fp =  output_dir + "valid_image_upcs"
pim.write.mode("overwrite").format("parquet").save(output_fp)
