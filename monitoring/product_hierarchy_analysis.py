# Databricks notebook source
"""
Creates inputs for the product hierarchy analysis sheet on the
Audience Monitoring Dahsboard.

1) Pull PIM to get commodity, sub-commodity, micro-department,
sub-department, department, product name, and product description
information.

2) For each commodity, get:
    a. top 100 words when looking at all UPCs' product name.
    b. top 100 words when looking at all UPCs' product description.
    c. a sample of 30 product belonging to that commodity.
    Product names are provided.
    d. a count of how many EHHNs bought that commodity.
    e. a count of how many onsite-eligible EHHNs bought that commodity.
    f. a count of how many offsite-eligible EHHNs bought that commodity.
    * 52 weeks of transactions (ACDS) are pulled in to calculate d, e, and f.
    * The latest eligibility fact file is used to calculate e and f.

3) For each sub-commodity, get all the same information listed in step 2
but do it at the sub-commodity level.

4) Output the product hierarchy from sub-commodity to department level
and output the commmodity + sub-commmodity breakdowns.
"""

# COMMAND ----------

#Packages Used
import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t
import pyspark.sql.window as w
import dateutil.relativedelta as dr
import datetime as dt

import resources.config as config
from effodata import ACDS, golden_rules, Joiner, Sifter, Equality, join_on 
from kpi_metrics import KPI, AliasMetric, CustomMetric, AliasGroupby

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
pim = pim.select(
  "department", "sub_department", "micro_department",
  "commodity", "sub_commodity",
  "gtin_no", "product_name", "product_description",
)
pim = pim.dropDuplicates(["gtin_no"])

#Initial cleaning the handles problematic characters
#Replace these characters with white space to preserve symbolism
replace_with_space_pattern = r'[\/\\\-_|\|\+]'
pim = pim.withColumn("product_name", f.regexp_replace(f.col("product_name"), replace_with_space_pattern, " "))
pim = pim.withColumn("product_description", f.regexp_replace(f.col("product_description"), replace_with_space_pattern, " "))

#Delete these b/c they do not carry interpretable significance
delete_pattern = r'[<>\?\!\;\:\}\{\=\&\%\$\#\@\~\^\*\(\)\[\]]'
pim = pim.withColumn("product_name", f.regexp_replace(f.col("product_name"), delete_pattern, ""))
pim = pim.withColumn("product_description", f.regexp_replace(f.col("product_description"), delete_pattern, ""))

#Created a bunch of white space - clean it up
pim = pim.withColumn("product_name", f.regexp_replace(f.col("product_name"), r'\s+', ' '))
pim = pim.withColumn("product_name", f.regexp_replace(f.col("product_name"), r'^\s+|\s+$', ''))
pim = pim.withColumn("product_description", f.regexp_replace(f.col("product_description"), r'\s+', ' '))
pim = pim.withColumn("product_description", f.regexp_replace(f.col("product_description"), r'^\s+|\s+$', ''))

#Standardizing format to use in top n analysis
pim = pim.withColumn("product_name_cleaned", f.lower(f.regexp_replace("product_name", "[@()\\[\\]/\\-_]", "")))
pim = pim.withColumn("product_description_cleaned", f.lower(f.regexp_replace("product_description", "[@()\\[\\]/\\-_]", "")))
pim = pim.withColumn("product_name_cleaned", f.trim(pim["product_name_cleaned"]))
pim = pim.withColumn("product_desciption_cleaned", f.trim(pim["product_description_cleaned"]))

#Product name has a bad habit of showing up empty.
#If it is empty, then just give it the value of the cleaned
#product description.
pim = pim.withColumn(
    "product_name",
    f.when(f.length(f.col("product_name")) == 0, f.col("product_description_cleaned"))
    .otherwise(f.col("product_name"))
)

#Hiearachy to attach at the end
hier = pim.select("department", "sub_department", "micro_department", "commodity", "sub_commodity").dropDuplicates()
hier = hier.orderBy(["department", "sub_department", "micro_department", "commodity", "sub_commodity"])


# COMMAND ----------

#For commodity level breakdown
commodity = pim.select(
  "commodity", "gtin_no",
  "product_name", "product_name_cleaned", "product_description_cleaned",
)
sub_commodity = pim.select(
  "commodity", "sub_commodity", "gtin_no",
  "product_name", "product_name_cleaned", "product_description_cleaned",
)
#n := how many top words to extract
n = 100
#s := how many products to sample
s = 30
#To store outputs is we do the loop at the commodity + sub_commodity level breakdowns
output_container = []
for df, column in zip([commodity, sub_commodity], ["commodity", "sub_commodity"]):
  
  ###Get top n words in product name
  #Get the complete word occurence in each commodity/sub_commodity
  word_count = df.select(column, f.explode(f.split(df["product_name_cleaned"], " ")).alias("word"))
  word_count = word_count.groupBy([column, "word"]).count()
  word_count = word_count.sort([column, "count"], ascending=False)

  #Get the top n words in the product name for each  commodity/sub_commodity
  windows = w.Window.partitionBy(column).orderBy(f.col("count").desc())
  top_n = word_count.withColumn("row", f.row_number().over(windows)).filter(f.col("row") <= n)
  top_n = top_n.groupBy(column).agg(f.collect_list('word').alias('top_words(name)'))
  #Turn list-columns into string columns
  top_n = top_n.withColumn('top_words(name)', f.concat_ws(", ", 'top_words(name)'))
  top_n = top_n.select(column, 'top_words(name)')

  top_n_name = top_n
  del(top_n)

  ###Get top n words in product description for each  commodity/sub_commodity
  #Get the complete word occurence in each commodity/sub_commodity
  word_count = df.select(column, f.explode(f.split(df["product_description_cleaned"], " ")).alias("word"))
  word_count = word_count.groupBy([column, "word"]).count()
  word_count = word_count.sort([column, "count"], ascending=False)

  #Get the top n words in the product name for each  commodity/sub_commodity
  windows = w.Window.partitionBy(column).orderBy(f.col("count").desc())
  top_n = word_count.withColumn("row", f.row_number().over(windows)).filter(f.col("row") <= n)
  top_n = top_n.groupBy(column).agg(f.collect_list('word').alias('top_words(description)'))
  #Turn list-columns into string columns
  top_n = top_n.withColumn('top_words(description)', f.concat_ws(", ", 'top_words(description)'))
  top_n = top_n.select(column, 'top_words(description)')

  top_n_desc = top_n
  del(top_n)

  ###Join top words in name + description
  output = top_n_name.join(top_n_desc, column, "outer")

  ###Provide product sample of s products
  #Get a sample of s products for each commodity/sub-commodity
  df = df.withColumn("dummy", f.lit(1))

  #
  product_windows = w.Window.partitionBy(column).orderBy(f.col("dummy"))
  product_samples = df.withColumn("row", f.row_number().over(product_windows)).filter(f.col("row") <= s)
  product_samples = product_samples.groupBy(column).agg(f.collect_list('product_name').alias('product_sample'))
  #Turn list-columns into string columns
  product_samples = product_samples.withColumn("product_sample", f.concat_ws(", ", "product_sample"))
  product_samples = product_samples.select(column, "product_sample")

  #Bring the product samples back to the output
  output = output.join(product_samples, column, "outer")

  #Put in container to extract after loop
  output_container += [output]
  del(output)

# COMMAND ----------

#Define lookback window for ACDS
max_weeks = 52
today = dt.date.today()
last_monday = today - dr.datetime.timedelta(days=today.weekday())
start_date = last_monday - dr.datetime.timedelta(weeks=max_weeks)
end_date = last_monday - dr.datetime.timedelta(days=1)

#Read in 52 weeks of ACDS
acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))
acds = acds.select("ehhn", "gtin_no")

#Put into a reusable function that takes in df with "ehhn" as one of the columns
#####################################################################
#Read in latest eligibility file
eligibility_path = ('abfss://landingzone@sa8451entlakegrnprd.dfs.core.windows.net/mart/comms/prd/fact/eligibility_fact')
files_path = dbutils.fs.ls(eligibility_path)
sorted_files = sorted(files_path, key=lambda x: x[1], reverse=True)

# Get the second latest file to read it in
second_latest_file = sorted_files[1]
print("The latest file is: ", second_latest_file.path)
eligibility = spark.read.parquet(second_latest_file.path)

overall_cols = [
  'NATIVE_ELIGIBLE_FLAG','TDC_ELIGIBLE_FLAG','SSE_ELIGIBLE_FLAG',
  'FFD_ELIGIBLE_FLAG','EMAIL_ELIGIBLE_FLAG','PUSH_FLAG',
  'FACEBOOK_FLAG','PANDORA_FLAG','CHICORY_FLAG','PUSH_FLAG',
  'PREROLL_VIDEO_ELIGIBLE_FLAG','PINTEREST_ELIGIBLE_FLAG','ROKU_FLAG',
]
onsite_cols = [
  'TDC_ELIGIBLE_FLAG', 'SSE_ELIGIBLE_FLAG',
  'FFD_ELIGIBLE_FLAG', 'SS_ELIGIBLE_FLAG',
  'PUSH_FLAG',
]
offsite_cols = [
  'FACEBOOK_FLAG', 'LIVERAMP_FLAG', 'PANDORA_FLAG',
  'PINTEREST_ELIGIBLE_FLAG', 'PREROLL_VIDEO_ELIGIBLE_FLAG',
  'CBA_ELIGIBLE_FLAG',
]

#Create eligibility flags if they have "Y" in any of their designated columns
#eligibility = eligibility.withColumn(
#    "overall_eligibility", 
#    f.when(f.expr(f"array_contains(array({','.join(overall_cols)}), 'Y')"), 1).otherwise(0)
#)

#Onsite eligible households
eligibility = eligibility.withColumn(
    "onsite_eligibility", 
    f.when(f.expr(f"array_contains(array({','.join(onsite_cols)}), 'Y')"), 1).otherwise(0)
)
onsite = eligibility.filter(f.col("onsite_eligibility") == 1)
onsite = onsite.select("ehhn", "onsite_eligibility").dropDuplicates()

#Offsite eligible households
eligibility = eligibility.withColumn(
    "offsite_eligibility", 
    f.when(f.expr(f"array_contains(array({','.join(offsite_cols)}), 'Y')"), 1).otherwise(0)
)
offsite = eligibility.filter(f.col("offsite_eligibility") == 1)
offsite = offsite.select("ehhn", "offsite_eligibility").dropDuplicates()

#Function to check if any column contains 'Y'
#def any_column_contains_Y(*args):
#    return 'Y' in args
# Register UDF
#contains_Y_udf = udf(any_column_contains_Y, t.BooleanType())
# Apply UDF to create new columns
#acds = acds.withColumn("overall_eligibility", contains_Y_udf(*[f.col(column) for column in overall_cols]))
#acds = acds.withColumn("onsite_eligibility", contains_Y_udf(*[f.col(column) for column in onsite_cols]))
#acds = acds.withColumn("offsite_eligibility", contains_Y_udf(*[f.col(column) for column in offsite_cols]))
#####################################################################

acds = acds.join(onsite, "ehhn", "left")
acds = acds.join(offsite, "ehhn", "left")
acds.cache()

# COMMAND ----------

output_container2 = []
for df, groupby_columns in zip([commodity, sub_commodity], [["commodity"], ["commodity", "sub_commodity"]]):
  #Merge commodity/sub-commodity designation to get raw counts
  ehhn_comm = acds.join(df, on="gtin_no", how="inner")
  ehhn_comm = ehhn_comm.\
    groupBy(groupby_columns).\
    agg(f.countDistinct("ehhn").alias("ehhn_count"))

  #Apply offsite filter to get offsite counts
  off_comm = acds.filter(f.col("offsite_eligibility") == 1)
  off_comm = off_comm.join(df, on="gtin_no", how="inner")
  off_comm = off_comm.\
    groupBy(groupby_columns).\
    agg(f.countDistinct("ehhn").alias("offsite_count"))
    
  #Apply onsite filter to get offsite counts
  on_comm = acds.filter(f.col("onsite_eligibility") == 1)
  on_comm = on_comm.join(df, on="gtin_no", how="inner")
  on_comm = on_comm.\
    groupBy(groupby_columns).\
    agg(f.countDistinct("ehhn").alias("onsite_count"))

  #Merge all 3 datasets into one
  output = ehhn_comm.\
  join(on_comm, groupby_columns, "outer").\
  join(off_comm, groupby_columns, "outer")

  #Anybody who was null in any count, just give them zero
  output = output.fillna({"ehhn_count": 0, "onsite_count": 0, "offsite_count": 0})

  #Put in container to extract after loop
  output_container2 += [output]
  del(output)

# COMMAND ----------

#Bring together data and output it
commodity_text = output_container[0]
commodity_counts = output_container2[0]
commodity = commodity_text.join(commodity_counts, "commodity", "inner")
commodity = commodity.select(
  "commodity",
  "ehhn_count", "onsite_count", "offsite_count",
  "top_words(name)", "top_words(description)", "product_sample",
)
commodity = commodity.orderBy(["commodity"])
commodity.cache()

#Bring together data and output it
sub_commodity_text = output_container[1]
sub_commodity_counts = output_container2[1]
sub_commodity = sub_commodity_text.join(sub_commodity_counts, ["sub_commodity"], "inner")
sub_commodity = sub_commodity.select(
  "commodity", "sub_commodity",
  "ehhn_count", "onsite_count", "offsite_count",
  "top_words(name)", "top_words(description)", "product_sample",
)
sub_commodity = sub_commodity.orderBy(["commodity", "sub_commodity"])
sub_commodity.cache()

# COMMAND ----------

import os

def write_out(df, fp, delim=",", fmt="csv"):
  """Writes out PySpark dataframe as a csv file
  that can be downloaded for Azure and loaded into
  Excel very easily.

  Example
  ----------
    write_out(df, "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/data_analysis.csv")

  Parameters
  ----------
  df: pyspark.sql.dataframe.DataFrame
    PySpark dataframe contains the data we'd like
    to conduct the group-by count on.

  fp: str
    String that the defines the column name of the 
    column of interest.

  delim: str
    String that specifies which delimiter to use in the
    write-out. Default value is ','.

  fmt: str
    String that specifies which format to use in the
    write-out. Default value is 'csv'.

  Returns
  ----------
  None. File is written out specified Azure location.
  """
  #Placeholder filepath for intermediate processing
  temp_target = os.path.dirname(fp) + "/" + "temp"

  #Write out df as partitioned file. Write out ^ delimited
  df.coalesce(1).write.options(header=True, delimiter=delim).mode("overwrite").format(fmt).save(temp_target)

  #Copy desired file from parititioned directory to desired location
  temporary_fp = os.path.join(temp_target, dbutils.fs.ls(temp_target)[3][1])
  dbutils.fs.cp(temporary_fp, fp)
  dbutils.fs.rm(temp_target, recurse=True)

fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/powerbi_inputs/product_hierarchy_stats/product_hierarchy"
hier.write.mode("overwrite").format("parquet").save(fp)
#write_out(hier, fp, delim="^")
fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/powerbi_inputs/product_hierarchy_stats/commodity_breakdown"
commodity.write.mode("overwrite").format("parquet").save(fp)
#write_out(commodity, fp, delim="^")
fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/powerbi_inputs/product_hierarchy_stats/subcommodity_breakdown"
sub_commodity.write.mode("overwrite").format("parquet").save(fp)
#write_out(sub_commodity, fp, delim="^")
