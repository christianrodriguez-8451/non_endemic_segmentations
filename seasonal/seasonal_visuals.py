# Databricks notebook source
"""
This is the code used to generate the box-and-whiskers plots,
ehhn counts plots, and commodity/sub-commodity appendix for the
seasonal audiences.
"""

# COMMAND ----------

# MAGIC %pip install protobuf==3.20.1

# COMMAND ----------

from effodata import ACDS, golden_rules

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import sklearn.preprocessing as sk_pre
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from matplotlib.gridspec import GridSpec
from scipy.stats import gaussian_kde
#from effodata import ACDS, golden_rules
import resources.config as config
from math import ceil
import os

# Initialize a Spark session
spark = SparkSession.builder.appName("seasonal_spending_visuals").getOrCreate()

def daily_bw_plot(vis, df, label):
  """
  """
# Aggregate data
  aggregated_df = df.\
    groupBy("date").\
    agg(f.collect_list("dollars_spent").alias("dollars_spent_list"))
  #Convert to Pandas DataFrame
  pandas_df = aggregated_df.toPandas()
  pandas_df = pandas_df.sort_values(by=["date"], ascending=True)
  pandas_df = pandas_df.reset_index(drop=True)
  #Explode the list column to separate rows
  exploded_df = pandas_df.explode('dollars_spent_list')
  #Rename columns for clarity
  exploded_df.columns = ['date', 'dollars_spent']
  #Formate the date column for readability
  exploded_df['date'] = exploded_df.astype(str)
  exploded_df['date'] = exploded_df['date'].apply(lambda x: f"{x[:4]}-{x[4:6]}-{x[6:]}")

  #Create the box-and-whiskers plot
  sns.boxplot(
    x='date', y='dollars_spent', data=exploded_df, showfliers=False,
    boxprops=dict(color='black', facecolor='white'),
    whiskerprops=dict(color='black'),
    capprops=dict(color='black'),
    medianprops=dict(color='black'),
    flierprops=dict(markerfacecolor='black', markeredgecolor='black'),
  )
  vis.set_title('Box-and-Whiskers Plot of Dollars Spent Across Time ({})'.format(label))
  vis.set_xlabel('Date')
  vis.set_ylabel('Dollars Spent')
  vis.set_xticklabels(vis.get_xticklabels(), rotation=60)

def daily_count_plot(vis, df, label):
  """
  """
  # Aggregate data
  aggregated_df = df.\
    groupBy("date").\
    agg(f.countDistinct("ehhn").alias("ehhn_count"))
  #Convert to Pandas DataFrame
  pandas_df = aggregated_df.toPandas()
  pandas_df = pandas_df.sort_values(by=["date"], ascending=True)
  pandas_df = pandas_df.reset_index(drop=True)
  #Formate the date column for readability
  pandas_df['date'] = pandas_df.astype(str)
  pandas_df['date'] = pandas_df['date'].apply(lambda x: f"{x[:4]}-{x[4:6]}-{x[6:]}")

  #Create the box-and-whiskers plot
  sns.lineplot(x='date', y='ehhn_count', data=pandas_df, marker="o")
  
  #Set title and axis names
  vis.set_title('EHHN Count Across Time ({})'.format(label))
  vis.set_xlabel('Date')
  vis.set_ylabel('EHHN Count')
  #Rotate tick values for readability
  vis.set_xticks(pandas_df['date'])
  vis.set_xticklabels(pandas_df['date'], rotation=60)
    
def seasonal_plots(daily_df, dept_df, fig):
  """
  """
  #Get all unique departments in dataset
  departments = dept_df.select("micro_department").dropDuplicates().collect()
  departments = [x["micro_department"] for x in departments]
  departments.sort()

  #There are plots for the overall level + each microdepartment
  #There are two plots for each - box-and-whiskers plot + scatter plot
  n_subplots = int((1 + len(departments))*2)
  #Plot the initial box-and-whisker plot. The overall spending plot.
  index = 1
  vis1 = fig.add_subplot(n_subplots, 1, index)
  daily_bw_plot(vis=vis1, df=daily_df, label="OVERALL")
  #Plot the initial scatter plot. The overall ehhn count plot.
  index += 1
  vis2 = fig.add_subplot(n_subplots, 1, index)
  daily_count_plot(vis=vis2, df=daily_df, label="OVERALL")

  #Plot the box-and-whisker on spending for the rest of the departments
  for dept in departments:
    temp = dept_df.filter(f.col("micro_department") == dept)

    index += 1
    visi = fig.add_subplot(n_subplots, 1, index)
    daily_bw_plot(vis=visi, df=temp, label=dept)

    index += 1
    visj = fig.add_subplot(n_subplots, 1, index)
    daily_count_plot(vis=visj, df=temp, label=dept)

def table(df, vis, loc, font_size=None):
  """
  """
  temp = df.toPandas()
  c_labels = list(temp.columns)
  temp = temp.to_numpy()
  r_labels = list(range(0, len(temp)))
  
  table = plt.table(cellText=temp, colLabels=c_labels, rowLabels=r_labels, cellLoc="center", loc=loc)
  #table = vis.table(cellText=temp, colLabels=c_labels, rowLabels=r_labels, cellLoc="center", loc="center")
  table.scale(1.5, 1.5)   # Scale the table (width, height)
  plt.axis('off')

  if font_size is None:
    table.auto_set_font_size(True)
  else:
    table.auto_set_font_size(False)
    table.set_fontsize(font_size)

# COMMAND ----------

#Read in the control file and have it ready for each
#audience's commodities + sub-commodities
fp = (
  "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" +
  "seasonal_commodities_control2.csv"
)
schema = t.StructType([
    t.StructField("holiday", t.StringType(), True),
    t.StructField("commodity", t.StringType(), True),
    t.StructField("sub_commodity", t.StringType(), True),
    t.StructField("seasonality_score", t.IntegerType(), True)
])
holiday_comms = spark.read.csv(fp, schema=schema, header=True)

#Separate commodity level choices from sub-commodities level choices
h_comms = holiday_comms.\
  filter(f.col("sub_commodity") == "ALL SUB-COMMODITIES").\
  select("holiday", "commodity", "seasonality_score")

h_scomms = holiday_comms.\
  filter(f.col("sub_commodity") != "ALL SUB-COMMODITIES").\
  select("holiday", "commodity", "sub_commodity", "seasonality_score")
h_scomms = h_scomms.withColumnRenamed("seasonality_score", "seasonality_sub_score")

#Read in PIM to give each commodity + sub-commodity their microdepartment
pim_fp = config.get_latest_modified_directory(config.azure_pim_core_by_cycle)
pim = config.spark.read.parquet(pim_fp)
pim = pim.select(
  f.col("upc").alias("gtin_no"),
  f.col("familyTree.commodity.name").alias("commodity"),
  f.col("familyTree.subCommodity.name").alias("sub_commodity"),
  f.col("familyTree.primaryDepartment.name").alias("department"),
  f.col("familyTree.primaryDepartment.recapDepartment.name").alias("sub_department"),
  f.col("familyTree.primaryDepartment.recapDepartment.department.name").alias("micro_department"),
)
pim = pim.select("commodity", "sub_commodity", "micro_department")
pim = pim.dropDuplicates(["commodity", "sub_commodity"])
pim.cache()

temp = pim.select(["commodity", "micro_department"]).dropDuplicates()
h_comms = h_comms.join(temp, "commodity", "left")
h_comms = h_comms.orderBy(["micro_department", "commodity"])
h_comms = h_comms.select("holiday", "micro_department", "commodity", "seasonality_score")
h_comms.cache()
del(temp)

h_scomms = h_scomms.join(pim, ["commodity", "sub_commodity"], "left")
h_scomms = h_scomms.orderBy(["micro_department", "commodity", "sub_commodity"])
h_scomms = h_scomms.select("holiday", "micro_department", "commodity", "sub_commodity", "seasonality_sub_score")
h_scomms.cache()
del(pim)

# COMMAND ----------

audiences = [
  "super_bowl",
  "valentines_day",
  "st_patricks_day",
  "easter",
  "may_5th",
  "mothers_day",
  "memorial_day",
  "fathers_day",
  "july_4th",
  "back_to_school",
  "labor_day",
  "halloween",
  "thanksgiving",
  "christmas_eve",
  "new_years_eve",
]

# COMMAND ----------


for audience in audiences:
  try:
    #Read in the audience's datasets for box-and-whiskers plots + ehhn-count line plots
    output_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + audience + "/"

    dept_fp = output_dir + "{}_microdepartment_spending".format(audience)
    dept_df = spark.read.format("delta").load(dept_fp)
    daily_fp = output_dir + "{}_daily_spending".format(audience)
    daily_df = spark.read.format("delta").load(daily_fp)
    ehhn_fp = output_dir + "{}_ehhn_spending".format(audience)
    ehhn_df = spark.read.format("delta").load(ehhn_fp)

    #ehhn_df = ehhn_df.limit(10000)
    #dept_df = dept_df.join(ehhn_df.select("ehhn"), "ehhn", "inner")
    #daily_df = daily_df.join(ehhn_df.select("ehhn"), "ehhn", "inner")

    #Get all unique departments in dataset to see how big the figure needs to be
    departments = dept_df.select("micro_department").dropDuplicates().collect()
    departments = [x["micro_department"] for x in departments]
    desired_height = int(len(departments) * 18)
    #Use the smaller of the desired height or the maximum height. Don't want to overload PDF
    max_height = 2**16
    fig_height = min(desired_height, max_height)
    fig_i = plt.figure(figsize=(12, fig_height))

    #Visualize spending-across-time at overall and department levels
    seasonal_plots(daily_df=daily_df, dept_df=dept_df, fig=fig_i)

    #Adjust for readability
    #Sometimes, visuals are too large to tighten
    try:
      plt.subplots_adjust(hspace=0.5)
      plt.tight_layout()
    except:
      print(f"Could not tighten visuals for {audience}.")

    #Initialize the PDF that will contain the audience's visuals
    dbfs_dir1 = "/dbfs/dbfs/FileStore/users/c127414/"
    fn = "visual_analysis_{}.pdf".format(audience)
    pdf = PdfPages(dbfs_dir1 + fn)
    #Save the figure to specified PDF
    pdf.savefig(fig_i)

    #Close the pdf and save it
    pdf.close()
    dbfs_dir2 = "dbfs:/dbfs/FileStore/users/c127414/"
    abfs_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"
    dbutils.fs.cp(dbfs_dir2 + fn, abfs_dir + fn)

  except:
    print(f"Skipped {audience}!")


# COMMAND ----------

for audience in audiences:
  
  height = (h_comms.filter(f.col("holiday") == audience).count() + h_scomms.filter(f.col("holiday") == audience).count())
  height = int(1*ceil(height/25))
  fig_j = plt.figure(figsize=(12, height))

  #Include the audience's chosen commodities + sub-commodities
  #in the PDF. Think of it as the appendix.
  temp = h_comms.filter(f.col("holiday") == audience)
  temp = temp.select("micro_department", "commodity", "seasonality_score")
  table(df=temp, vis=fig_j, loc="top")
  del(temp)

  temp = h_scomms.filter(f.col("holiday") == audience)
  temp = temp.select("micro_department", "commodity", "sub_commodity", "seasonality_sub_score")
  table(df=temp, vis=fig_j, loc="bottom")
  del(temp)

  #Initialize the PDF that will contain the audience's visuals
  dbfs_dir1 = "/dbfs/dbfs/FileStore/users/c127414/"
  fn = "visual_analysis_{}_appendix.pdf".format(audience)
  pdf = PdfPages(dbfs_dir1 + fn)
  #Save the figure to specified PDF
  pdf.savefig(fig_j, bbox_inches='tight')

  #Close the pdf and save it
  pdf.close()
  dbfs_dir2 = "dbfs:/dbfs/FileStore/users/c127414/"
  abfs_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"
  dbutils.fs.cp(dbfs_dir2 + fn, abfs_dir + fn)

# COMMAND ----------

i = 0
for audience in audiences:

  #Read in the audience's datasets for box-and-whiskers plots + ehhn-count line plots
  output_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + audience + "/"

  dept_fp = output_dir + "{}_microdepartment_spending".format(audience)
  dept_df = spark.read.format("delta").load(dept_fp)

  #For the given holiday, calculate daily spending in each micro-department
  daily_ehhn_counts = dept_df.\
  groupBy(["micro_department", "date"]).\
  agg(f.countDistinct("ehhn").alias("ehhn_count"))

  #Identify the 4 days leading up to the holiday (including the holiday)
  leadup_dates = daily_ehhn_counts.\
  select("date").\
  dropDuplicates().\
  orderBy(f.col("date").desc()).\
  limit(4)
  leadup_dates = leadup_dates.withColumn("leadup_date", f.lit(1))

  #Calculate cut-offs for PASS, ???, BLOCK recommendations
  microdept_stats = daily_ehhn_counts.\
  groupBy("micro_department").\
  agg(
    f.mean("ehhn_count").alias("avg_ehhn_count"),
    f.stddev("ehhn_count").alias("std_ehhn_count"),
  )
  #95th percentile cut-off
  pass_cutoff = 1.645
  microdept_stats = microdept_stats.withColumn("pass_threshold", f.col("avg_ehhn_count") + (f.lit(pass_cutoff) * f.col("std_ehhn_count")))
  #80th percentile cut-off
  unsure_cutoff = 0.842
  microdept_stats = microdept_stats.withColumn("unsure_threshold", f.col("avg_ehhn_count") + (f.lit(unsure_cutoff) * f.col("std_ehhn_count")))

  #Check if there is a spike in ehhns in the 4 days leading up to holiday
  daily_ehhn_counts = daily_ehhn_counts.join(microdept_stats,
                                            "micro_department",
                                            "inner")
  daily_ehhn_counts = daily_ehhn_counts.join(leadup_dates,
                                            "date",
                                            "left")

  #Identify micro-departments that look PASSABLE
  recommend_pass = daily_ehhn_counts.\
  filter(
    (f.col("leadup_date") == 1) & (f.col("ehhn_count") >= f.col("pass_threshold"))
  ).\
  select("micro_department").\
  dropDuplicates().\
  orderBy("micro_department")
  recommend_pass = recommend_pass.withColumn("pass", f.lit(1))

  #Identify micro-departments that look ???
  recommend_unsure = daily_ehhn_counts.\
  filter(
    (f.col("leadup_date") == 1) & (f.col("ehhn_count") < f.col("pass_threshold")) & (f.col("ehhn_count") >= f.col("unsure_threshold"))
  ).\
  join(recommend_pass, "micro_department", "left").\
  filter(f.col("pass").isNull()).\
  select("micro_department").\
  dropDuplicates().\
  orderBy("micro_department")
  recommend_unsure = recommend_unsure.withColumn("unsure", f.lit(1))

  #Otherwise, recommend to block these micro-departments
  recommend_block = daily_ehhn_counts.\
  select("micro_department").\
  dropDuplicates().\
  join(recommend_pass, "micro_department", "left").\
  join(recommend_unsure, "micro_department", "left").\
  filter((f.col("pass").isNull())&(f.col("unsure").isNull())).\
  select("micro_department").\
  orderBy("micro_department")

  #Stitch all recommendations together
  recommend_pass = recommend_pass.select("micro_department").withColumn("recommendation", f.lit("PASS"))
  recommend_unsure = recommend_unsure.select("micro_department").withColumn("recommendation", f.lit("???"))
  recommend_block = recommend_block.select("micro_department").withColumn("recommendation", f.lit("BLOCK"))

  recommendations = recommend_pass.union(recommend_unsure).union(recommend_block)
  recommendations = recommendations.withColumn("holiday", f.lit(audience))
  recommendations = recommendations.select("holiday", "micro_department", "recommendation")
  recommendations = recommendations.withColumn("verdict", f.col("recommendation"))
  #Still all holiday recommendations together
  if i == 0:
    all_recommendations = recommendations
  else:
    all_recommendations = all_recommendations.union(recommendations)

  i += 1

################################################################
###
################################################################

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

fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/seasonal_dept_recs.csv"
write_out(all_recommendations, fp)

