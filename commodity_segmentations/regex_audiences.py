# Databricks notebook source
"""
Reads in the regex-segmentations dictionary from config.py and
pulls in PIM to assign each product their department, sub-department,
micro-department, commodity, sub-commodity, product name, and
product description from the UPC hierarchy. After PIM is pulled,
we impose filters and regular expressions (RegEx) to pull the desired
UPCs for the given segmentation. A delta file is written out for
each segmenation's extracted UPCs.

The key-value pair for "pale_ale" from the regex_segmenetations
dictionary has been provided below to illustrate the logic for
each entry within the dictionary.

  "pale_ale": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["pale"], [" ale"]],
    "must_not_contain": [["ipa", "indian", "india"]],
    "composes": "ale",

In the case of "pale_ale", we first use PIM to filter for strictly UPCs under the
BEER micro-department. The purpose of this initial filter is to make sure we
only get beer products after we do implement RegEx. Next, we use RegEx to keep
only the UPCs that contain "pale" and also contain " ale" in either the product name
or product description. "pale_ale" is a special exception where it also contains a 
key for "must_not_contain". After applying our first RegEx search, we apply a second one
and exclude any UPCs that contain any of the following in the product name or product
description: "ipa", "indian", and "india". The reason for this is because when we 
implement first RegEx search, we pick up a lot of products that are india(n) pale
ales. India(n) pale ales and pale ales are beer categories that are distinct
from one another within the market place. The last key "pale_ale" has is "composes".
This is an entry to indicate if the given segmentation is a component for the
specified segmentation. In this case, "pale_ale" is a component for the
"ale" segmentation. Composite segmentations go through the filtering
and RegEx like all other segmentations, but the composite segmentation's
UPC list and household list are unioned with its components' UPC lists
and household lists.

After we've collected the desired UPCs for the given segmentation,
we then pull 26 weeks of the latest transaction data via ACDS. For the
given segmentation, we only keep households that have bought at least
one of the desired UPCs. Finally, we write out the set of households
as a delta file for each given segmentation.

The reason for this specific "26-week-buyers-of-X" approach is because the
KPM guidelines prohibits from any behavior based targeting and 26 weeks is the max
look back window that is allowed alcoholic products based targeting.
"""

# COMMAND ----------

"""
CODE USED TO EXPERIMENT AND EXPLORE COMBINATIONS

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
# Convert to lowercase and remove specified special characters
pim = pim.dropDuplicates(["gtin_no"])
pim = pim.withColumn("product_name", f.lower(f.regexp_replace("product_name", "[@()\\[\\]/\\-_]", "")))
pim = pim.withColumn("product_description", f.lower(f.regexp_replace("product_description", "[@()\\[\\]/\\-_]", "")))

# List of patterns
str_list = ["lemonade"]
# Combine patterns into a single regex pattern
regex_pattern = "|".join(str_list)
# Filter the DataFrame to keep only rows where col_x contains one of the patterns
pim = pim.filter(f.col("product_name").rlike(regex_pattern)|f.col("product_description").rlike(regex_pattern))
pim = pim.filter(f.col("sub_department") == "LIQUOR")
pim.limit(1000).display()
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

#Import packages I am relying on
import resources.config as config
import pyspark.sql.functions as f
from datetime import datetime, timedelta, date
from effodata import ACDS, golden_rules, joiner, sifter, join_on

import pyspark.sql.types as t
import commodity_segmentations.config as con

# COMMAND ----------

#Import dictionary that
regex_dict = con.regex_segmentations

#Read in PIM and keep product name, product description, commodity,
#sub-commodity, department, and sub-department.
#Add brand?
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

#Convert to lowercase and remove specified special characters
#Makes querying easy
pim = pim.dropDuplicates(["gtin_no"])
pim = pim.withColumn("product_name", f.lower(f.regexp_replace("product_name", "[@()\\[\\]/\\-_]", "")))
pim = pim.withColumn("product_description", f.lower(f.regexp_replace("product_description", "[@()\\[\\]/\\-_]", "")))

#Imposing this filter so that is speeds up querying down the line.
#We should consider the below line as we open up the methodology
#to non Alcoholic Beverage themes
pim = pim.filter(f.col("sub_department") == "LIQUOR")

#Cache for the heavy lifting down the road
pim.cache()

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
acds = acds.filter(f.col("week") <= 26)

#Apply Al-Bev filter to speed querying
acds = acds.join(pim.select("gtin_no"), "gtin_no", "inner")

#Cache for the heavy lifting down the road
#acds = acds.limit(500000)
acds.cache()

# COMMAND ----------

#Declare schema for dataframe that we will be appending to
ehhn_schema = t.StructType([t.StructField("ehhn", t.IntegerType(), True)])
upc_schema = t.StructType([t.StructField("gtin_no", t.StringType(), True)])
#Create empty dataframes for ale and lager (one for household and one for upc)
ale = spark.createDataFrame(spark.sparkContext.emptyRDD(), ehhn_schema)
ale_upcs = spark.createDataFrame(spark.sparkContext.emptyRDD(), upc_schema)
lager = spark.createDataFrame(spark.sparkContext.emptyRDD(), ehhn_schema)
lager_upcs = spark.createDataFrame(spark.sparkContext.emptyRDD(), upc_schema)

for alc in list(regex_dict.keys()):
  alc_dict = regex_dict[alc]

  #Apply filter to isolate regex search
  filter_col = list(alc_dict["filter"].keys())[0]
  filter_value = alc_dict["filter"][filter_col]
  alc_upcs = pim.filter(f.col(filter_col).isin(filter_value))

  #Keep UPC products that contain the desired regex patterns in the product name or product description
  for must_conditions in alc_dict["must_contain"]:
    #For each nested list, ***
    regex_pattern = "|".join(must_conditions)
    #If either product name or product description contains one of the desired patterns, then keep the row
    alc_upcs = alc_upcs.filter(f.col("product_name").rlike(regex_pattern)|f.col("product_description").rlike(regex_pattern))

  #Drop UPC products that contain the undesired regex pattern in the product name or product description
  if "must_not_contain" in list(alc_dict.keys()):
    for must_not_conditions in alc_dict["must_not_contain"]:
      #For each nested list, ***
      regex_pattern = "|".join(must_not_conditions)
      #If either product name or product description contains one of the undesired patterns, then drop the row
      alc_upcs = alc_upcs.filter(~(f.col("product_name").rlike(regex_pattern)|f.col("product_description").rlike(regex_pattern)))

  alc_upcs.cache()

  #Pull product sample for validation
  product_sample = alc_upcs.\
  select("product_description").\
  limit(50).\
  collect()
  product_sample = [x["product_description"] for x in product_sample]
  product_sample = ", ".join(product_sample)
  message = f"Product sample for {alc}: {product_sample}"
  print(message)

  #Keep only transactions that have the handpicked UPCs
  alc_upcs = alc_upcs.select("gtin_no").dropDuplicates()
  ehhns = acds.join(alc_upcs, "gtin_no", "inner")
  #Collect the unique households that bought from handpicked UPCs
  ehhns = ehhns.select("ehhn").dropDuplicates()
  ehhns = ehhns.cache()

  #Print upc counts and household counts as preliminary check
  message = f"UPC count for {alc}: {alc_upcs.count()}"
  print(message)
  message = f"Household count for {alc}: {ehhns.count()}"
  print(message)

  #Write out the upc and household files to their expected location
  if alc not in ["ale", "lager"]:
    backend_name = alc.\
      lower().\
      replace(" ", "_").\
      replace("-", "_")

    #Write out UPC file
    ehhns = ehhns.select("ehhn")
    ehhns = ehhns.withColumn("segment", f.lit("H"))
    fp = f'{con.output_fp}sensitive_segmentations/{backend_name}/{backend_name}_{today}'
    ehhns.write.mode("overwrite").format("delta").save(fp)
    print(f"Successfully wrote out {fp}!")
    del(fp)

    #Write out household file
    alc_upcs = alc_upcs.select("gtin_no")
    fp =  f'{con.output_fp}upc_lists/{backend_name}/{backend_name}_{today}'
    alc_upcs.write.mode("overwrite").format("delta").save(fp)
    print(f"Successfully wrote out {fp}!")
    del(fp)

  #Union the pieces into the greater composite
  if "composes" in list(alc_dict.keys()):
    #Ale composition
    if alc_dict["composes"] == "ale":
      ale = ale.union(ehhns.select("ehhn"))
      ale = ale.dropDuplicates()
      ale.cache()

      ale_upcs = ale_upcs.union(alc_upcs.select("gtin_no"))
      ale_upcs = ale_upcs.dropDuplicates()
      ale_upcs.cache()

    #Lager composition
    if alc_dict["composes"] == "lager":
      lager = lager.union(ehhns.select("ehhn"))
      lager = lager.dropDuplicates()
      lager.cache()

      lager_upcs = lager_upcs.union(alc_upcs.select("gtin_no"))
      lager_upcs = lager_upcs.dropDuplicates()
      lager_upcs.cache()

  #Clear for next iteration
  del(alc_upcs, ehhns)


# COMMAND ----------

#Get quick counts on ale
message = f"UPC count for ale: {ale_upcs.count()}"
print(message)

message = f"Household count for ale: {ale.count()}"
print(message)

#Write out upc file
fp =  f'{con.output_fp}upc_lists/ale/ale_{today}'
ale_upcs.write.mode("overwrite").format("delta").save(fp)
print(f"Successfully wrote out {fp}!")
del(fp)

#Write out the household file
ale = ale.select("ehhn")
ale = ale.withColumn("segment", f.lit("H"))
fp = f'{con.output_fp}sensitive_segmentations/ale/ale_{today}'
ale.write.mode("overwrite").format("delta").save(fp)
print(f"Successfully wrote out {fp}!")
del(fp)

#Get quick counts on lager
message = f"UPC count for lager: {lager_upcs.count()}"
print(message)

message = f"Household count for lager: {lager.count()}"
print(message)

#Write out the upc file 
fp =  f'{con.output_fp}upc_lists/lager/lager_{today}'
lager_upcs.write.mode("overwrite").format("delta").save(fp)
print(f"Successfully wrote out {fp}!")
del(fp)

#Write out the household file
lager = lager.select("ehhn")
lager = lager.withColumn("segment", f.lit("H"))
fp = f'{con.output_fp}sensitive_segmentations/lager/lager_{today}'
lager.write.mode("overwrite").format("delta").save(fp)
print(f"Successfully wrote out {fp}!")
del(fp)
