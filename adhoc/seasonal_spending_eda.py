# Databricks notebook source
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sklearn.preprocessing as sk_pre
from effodata import ACDS, golden_rules
import resources.config as config
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

# Initialize a Spark session
spark = SparkSession.builder.appName("seasonal_spending").getOrCreate()

def daily_bw_plot(vis, df, label):
# Aggregate data
  aggregated_df = df.\
    groupBy("date").\
    agg(f.collect_list("dollars_spent").alias("dollars_spent_list"))
  # Convert to Pandas DataFrame
  pandas_df = aggregated_df.toPandas()
  pandas_df = pandas_df.sort_values(by=["date"], ascending=True)
  pandas_df = pandas_df.reset_index(drop=True)
  # Explode the list column to separate rows
  exploded_df = pandas_df.explode('dollars_spent_list')
  # Rename columns for clarity
  exploded_df.columns = ['date', 'dollars_spent']

  # Create the box-and-whiskers plot
  sns.boxplot(x='date', y='dollars_spent', data=exploded_df, showfliers=False)
  vis.set_title('Box-and-Whiskers Plot of Dollars Spent Across Time ({})'.format(label))
  vis.set_xlabel('Date')
  vis.set_ylabel('Dollars Spent')
  dates = pandas_df["date"]
  #vis.set_xticks(dates)
  #plt.setp(vis.get_xticklabels(), rotation=45, ha="right")

def seasonal_plots(daily_df, dept_df, fig, gs, starting_index):
  """
  """
  vis1 = fig.add_subplot(gs[starting_index, :])
  daily_bw_plot(vis=vis1, df=daily_df, label="OVERALL")

  #Get all unique departments in dataset
  departments = dept_df.select("department").dropDuplicates().collect()
  departments = [x["department"] for x in departments]
  for dept in departments:
    temp = dept_df.filter(f.col("department") == dept)

    starting_index += 1
    visi = fig.add_subplot(gs[starting_index, :])
    daily_bw_plot(vis=visi, df=temp, label=dept)

def percentile_clustering(df, num_col):
  #Calculate 33rd and 66th percentile cut-offs
  percentiles = ehhn_df.approxQuantile(num_col, [0.33, 0.66], 0.001)
  x33 = percentiles[0]
  x66 = percentiles[1]
  df = df.withColumn(
    "percentile_class",
    f.when(f.col(num_col) < x33, "L")
          .otherwise(f.when(f.col(num_col) > x66, "H")
                    .otherwise("M"))
  )
  return([df, percentiles])

def kmean_clustering(df, num_col):
  #Calculate cut-off for cluster based method
  #Covert into pandas dataframe to leverage K-Mean from sklearn
  pandas_df = df.toPandas()
  data = pandas_df[[num_col]]
  #Train K-means and assign cluster
  kmeans = KMeans(n_clusters=2, random_state=42)
  pandas_df['cluster'] = kmeans.fit_predict(data)
  #Convert Pandas DataFrame to PySpark DataFrame to extract cut-off via K-Means
  cluster_df = spark.createDataFrame(pandas_df)
  cluster_df = cluster_df.groupBy("cluster").agg(
    f.min(num_col).alias(f"min_{num_col}"),
    f.avg(num_col).alias(f"avg_{num_col}"),
    f.max(num_col).alias(f"max_{num_col}"),
    f.count("ehhn").alias("ehhn_count"),
  )
  cut_off = cluster_df.agg(f.max(f"min_{num_col}").alias("cut_off")).collect()[0]["cut_off"]
  #Assign cluster based on cut-off
  #We could've just pulled this from pandas_df, but we want the cut-off for data visuals
  df = df.withColumn(
    "cluster_class",
    f.when(f.col(num_col) < cut_off, "REGULAR")
          .otherwise("HOLIDAY")
  )
  return([df, cut_off])

def kde_plot(vis, df, num_col, perc_cutoff1, perc_cutoff2, cluster_cutoff):
  # Collect data from Spark DataFrame
  data_collected = df.select(num_col).rdd.flatMap(lambda x: x).collect()
  # Compute the kernel density estimate
  kde = gaussian_kde(data_collected)
  # Create a range of values
  x_range = np.linspace(min(data_collected), max(data_collected), 10000)
  # Evaluate the KDE over the range of values
  kde_values = kde(x_range)

  temp = num_col.replace("_", " ").title()
  vis.plot(x_range, kde_values, label='KDE')
  vis.fill_between(x_range, kde_values, alpha=0.5)
  vis.set_title(f'KDE of {temp}')
  vis.set_xlabel('Value')
  vis.set_ylabel('Density')
  #Place cut-offs for 33rd/66th percentiles
  vis.axvline(x=perc_cutoff1, color='r', linestyle='--', linewidth=2)
  vis.axvline(x=perc_cutoff2, color='r', linestyle='--', linewidth=2)
  #Place cut-off from clustering based method
  vis.axvline(x=cluster_cutoff, color='b', linestyle='--', linewidth=2)
  vis.set_xlim(0, max([perc_cutoff1, perc_cutoff2, cluster_cutoff])*3)
  vis.legend()

def kde_plot_by_group(vis, df, num_col, group_col, x_limit):
  df = df.toPandas()
  groups = df.groupby(group_col)
  colors = plt.cm.viridis(np.linspace(0, 1, len(groups)))
  
  for (name, group), color in zip(groups, colors):
      # Compute the KDE for the current group
      kde = gaussian_kde(group[num_col])
      
      # Create a range of values
      x_range = np.linspace(group[num_col].min(), group[num_col].max(), 10000)
      
      # Evaluate the KDE over the range of values
      kde_values = kde(x_range)

      group_count = group.shape[0]
      
      # Plot the KDE with shading and color
      vis.plot(x_range, kde_values, label=f'Group {name} (n={group_count})', color=color)
      vis.fill_between(x_range, kde_values, alpha=0.6, color=color)

  # Step 6: Add labels and title
  vis.set_xlim(0, x_limit*3)
  temp1 = num_col.replace("_", " ").title()
  temp2 = group_col.replace("_", " ").title()
  vis.set_title(f'KDE of {temp1} by {temp2}')
  vis.set_xlabel('Value')
  vis.set_ylabel('Density')
  vis.legend()

def table(df, index, fig):
  temp = df.toPandas()
  c_labels = list(temp.columns)
  temp = temp.to_numpy()
  r_labels = list(range(0, len(temp)))

  vis = fig.add_subplot(gs[index, :])
  vis.xaxis.set_visible(False)
  vis.yaxis.set_visible(False)
  vis.set_frame_on(False)
  
  table = vis.table(cellText=temp, colLabels=c_labels, rowLabels=r_labels)
  table.auto_set_font_size(True)
  #table.auto_set_font_size(False)
  #table.set_fontsize(10)
  table.scale(1.2, 1.2)

# COMMAND ----------

#Read in datasets by holiday
audience = "july_4th"
output_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + audience + "/"

dept_fp = output_dir + "{}_department_spending".format(audience)
dept_df = spark.read.format("delta").load(dept_fp)
dept_df.cache()

daily_fp = output_dir + "{}_daily_spending".format(audience)
daily_df = spark.read.format("delta").load(daily_fp)
daily_df.cache()

ehhn_fp = output_dir + "{}_ehhn_spending".format(audience)
ehhn_df = spark.read.format("delta").load(ehhn_fp)
ehhn_df.cache()

#ehhn_df = ehhn_df.limit(1000)
#ehhn_df.cache()
#dept_df = dept_df.join(ehhn_df.select("ehhn"), "ehhn", "inner")
#dept_df.cache()
#daily_df = daily_df.join(ehhn_df.select("ehhn"), "ehhn", "inner")
#daily_df.cache()

# COMMAND ----------

fp = (
  "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" +
  "seasonal_commodities_control.csv"
)
schema = t.StructType([
    t.StructField("holiday", t.StringType(), True),
    t.StructField("commodity", t.StringType(), True),
    t.StructField("sub_commodity", t.StringType(), True),
    t.StructField("seasonality_score", t.IntegerType(), True)
])
holiday_comms = spark.read.csv(fp, schema=schema, header=True)
holiday_comms = holiday_comms.filter(f.col("holiday") == audience)

#Separate commodity level choices from sub-commodities level choices
h_comms = holiday_comms.\
  filter(f.col("sub_commodity") == "ALL SUB-COMMODITIES").\
  select("commodity", "seasonality_score")
h_comms.cache()

h_scomms = holiday_comms.\
  filter(f.col("sub_commodity") != "ALL SUB-COMMODITIES").\
  select("commodity", "sub_commodity", "seasonality_score")
h_scomms = h_scomms.withColumnRenamed("seasonality_score", "seasonality_sub_score")
h_scomms.cache()

#Read in PIM and keep commodity,
#sub-commodity, department, and sub-department.
pim_fp = config.get_latest_modified_directory(config.azure_pim_core_by_cycle)
pim = config.spark.read.parquet(pim_fp)
pim = pim.select(
  f.col("familyTree.commodity.name").alias("commodity"),
  f.col("familyTree.subCommodity.name").alias("sub_commodity"),
  f.col("familyTree.primaryDepartment.name").alias("department"),
)
#For assigning each gtin_no their commodity and sub-commodity
pim = pim.select("commodity", "sub_commodity", "department")
pim = pim.dropDuplicates(["commodity", "sub_commodity"])
pim.cache()

temp = pim.select(["commodity", "department"]).dropDuplicates()
h_comms = h_comms.join(temp, "commodity", "left")
h_comms = h_comms.orderBy(["department", "commodity"])
h_comms = h_comms.select("department", "commodity", "seasonality_score")
del(temp)

h_scomms = h_scomms.join(pim, ["commodity", "sub_commodity"], "left")
h_scomms = h_scomms.orderBy(["department", "commodity", "sub_commodity"])
h_scomms = h_scomms.select("department", "commodity", "sub_commodity", "seasonality_sub_score")

h_comms.show(10, truncate=False)
h_scomms.show(10, truncate=False)

# COMMAND ----------

from matplotlib.gridspec import GridSpec
from scipy.stats import gaussian_kde
import numpy as np

#Calculate how many departments in given holiday commodities/sub-commodities
#to set how many rows will be needed in grid visual
departments = dept_df.select("department").dropDuplicates().collect()
departments = [x["department"] for x in departments]
departments = list(set(departments))
n_depts = len(departments)
n_rows = (4 + 1 + n_depts)
if h_comms.count() > 0:
  n_rows += 1

if h_scomms.count() > 0:
  n_rows += 1

#Define the grid we will be laying our visuals on
fig = plt.figure(figsize=(10, 30))
gs = GridSpec(nrows=n_rows, ncols=2, height_ratios=[3]*n_rows, width_ratios=[1]*2)

#Do percentile clustering by dollars spent
output = percentile_clustering(df=ehhn_df, num_col="dollars_spent")
ehhn_df = output[0]
percentiles = output[1]
x33 = percentiles[0]
x66 = percentiles[1]
del(output, percentiles)

#Do kmean clustering by dollars spent
output = kmean_clustering(df=ehhn_df, num_col="dollars_spent")
ehhn_df = output[0]
cut_off = output[1]
del(output)

#Visualize dollars spent distribution at overall and group levels
vis1 = fig.add_subplot(gs[0, :])
kde_plot(vis=vis1, df=ehhn_df, num_col="dollars_spent", perc_cutoff1=x33, perc_cutoff2=x66, cluster_cutoff=cut_off)
vis2 = fig.add_subplot(gs[1, 0])
kde_plot_by_group(vis=vis2, df=ehhn_df, num_col="dollars_spent", group_col="percentile_class", x_limit=x66)
vis3 = fig.add_subplot(gs[1, 1])
kde_plot_by_group(vis=vis3, df=ehhn_df, num_col="dollars_spent", group_col="cluster_class", x_limit=cut_off)

#Reset the dataframe
ehhn_df = ehhn_df.select("ehhn", "dollars_spent", "weighted_dollars_spent")
del(x33, x66, cut_off)

#Do percentile clustering by weighted dollars spent
output = percentile_clustering(df=ehhn_df, num_col="weighted_dollars_spent")
ehhn_df = output[0]
percentiles = output[1]
x33 = percentiles[0]
x66 = percentiles[1]
del(output, percentiles)

#Do kmean clustering by weighted dollars spent
output = kmean_clustering(df=ehhn_df, num_col="weighted_dollars_spent")
ehhn_df = output[0]
cut_off = output[1]
del(output)

#Visualize weighted dollars spent distribution at overall and group levels
vis4 = fig.add_subplot(gs[2, :])
kde_plot(vis=vis4, df=ehhn_df, num_col="weighted_dollars_spent", perc_cutoff1=x33, perc_cutoff2=x66, cluster_cutoff=cut_off)
vis5 = fig.add_subplot(gs[3, 0])
kde_plot_by_group(vis=vis5, df=ehhn_df, num_col="weighted_dollars_spent", group_col="percentile_class", x_limit=x66)
vis6 = fig.add_subplot(gs[3, 1])
kde_plot_by_group(vis=vis6, df=ehhn_df, num_col="weighted_dollars_spent", group_col="cluster_class", x_limit=cut_off)

#Visualize spending-across-time at overall and department levels
seasonal_plots(daily_df=daily_df, dept_df=dept_df, fig=fig, gs=gs, starting_index=4)

if (h_comms.count() > 0) & (h_scomms.count() == 0):
  table(h_comms, index=n_rows-1, fig=fig)

elif (h_comms.count() == 0) & (h_scomms.count() > 0):
  table(h_scomms, index=n_rows-1, fig=fig)

elif (h_comms.count() > 0) & (h_scomms.count() > 0):
  table(h_comms, index=n_rows-2, fig=fig)
  table(h_scomms, index=n_rows-1, fig=fig)

#Show grid visual
plt.tight_layout()
output_fig = plt.gcf()
plt.show()

#Write visuals as pdf
fn = "visual_analysis_{}.pdf".format(audience)
dbfs_dir1 = "/dbfs/dbfs/FileStore/users/c127414/"
dbfs_dir2 = "dbfs:/dbfs/FileStore/users/c127414/"
abfs_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"

output_fig.savefig(dbfs_dir1 + fn, format="pdf", bbox_inches="tight")
dbutils.fs.cp(dbfs_dir2 + fn, abfs_dir + fn)
