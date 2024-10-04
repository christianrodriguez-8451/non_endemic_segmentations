# Databricks notebook source
# MAGIC %pip install protobuf==3.20.*

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
from effodata import ACDS, golden_rules
import resources.config as config

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
  # Formate the date column for readability
  exploded_df['date'] = exploded_df.astype(str)
  exploded_df['date'] = exploded_df['date'].apply(lambda x: f"{x[:4]}-{x[4:6]}-{x[6:]}")

  # Create the box-and-whiskers plot
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
  #dates = pandas_df["date"]
  #vis.set_xticks(dates)
  #plt.setp(vis.get_xticklabels(), rotation=45, ha="right")

def seasonal_plots(daily_df, dept_df, fig, gs, starting_index):
  """
  """
  vis1 = fig.add_subplot(gs[starting_index, :])
  daily_bw_plot(vis=vis1, df=daily_df, label="OVERALL")

  #Get all unique departments in dataset
  departments = dept_df.select("micro_department").dropDuplicates().collect()
  departments = [x["micro_department"] for x in departments]
  for dept in departments:
    temp = dept_df.filter(f.col("micro_department") == dept)

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
    f.when(f.col(num_col) < cut_off, "Cluster A")
          .otherwise("Cluster B")
  )
  return([df, cut_off])

def gcard_counts(gcard_ehhns, ehhn_df, group_col):
  """
  """
  gcard_ehhns = gcard_ehhns.withColumn("bought_greeting_card", f.lit("Yes"))
  counts = gcard_ehhns.join(ehhn_df, "ehhn", "outer")
  counts = counts.fillna({
    "bought_greeting_card": "No",
    group_col: "No group assignment (not present in spending data)",
  })
  counts = counts.\
  groupby("bought_greeting_card", group_col).\
  agg(
    f.count(group_col).alias("ehhn_count"),
  )
  
  return(counts)

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
      vis.plot(x_range, kde_values, label=f'{name} (n={group_count})', color=color)
      vis.fill_between(x_range, kde_values, alpha=0.6, color=color)

  # Step 6: Add labels and title
  vis.set_xlim(0, x_limit*3)
  temp1 = num_col.replace("_", " ").title()
  temp2 = group_col.replace("_", " ").title()
  vis.set_title(f'KDE of {temp1} by {temp2}')
  vis.set_xlabel('Value')
  vis.set_ylabel('Density')
  vis.legend()

def table(df, vis, font_size=None):
  temp = df.toPandas()
  c_labels = list(temp.columns)
  temp = temp.to_numpy()
  r_labels = list(range(0, len(temp)))

  vis.axis('tight')
  vis.axis('off')
  
  table = vis.table(cellText=temp, colLabels=c_labels, rowLabels=r_labels, cellLoc="center", loc="center")
  table.scale(1, 1)

  if font_size is None:
    table.auto_set_font_size(True)
  else:
    table.auto_set_font_size(False)
    table.set_fontsize(font_size)

def daily_bw_plot(vis, df, label):
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
  #dates = pandas_df["date"]
  #vis.set_xticks(dates)
  #plt.setp(vis.get_xticklabels(), rotation=45, ha="right")

def seasonal_plots(daily_df, dept_df, fig):
  """
  """
  #Get all unique departments in dataset
  departments = dept_df.select("micro_department").dropDuplicates().collect()
  departments = [x["micro_department"] for x in departments]

  #
  n_subplots = 1 + len(departments)
  index = 1
  #Plot the initial box-and-whisker plot. The overall spending plot.
  vis1 = fig.add_subplot(n_subplots, 1, index)
  daily_bw_plot(vis=vis1, df=daily_df, label="OVERALL")

  #Plot the box-and-whisker on spending for the rest of the departments
  for dept in departments:
    temp = dept_df.filter(f.col("micro_department") == dept)

    index += 1
    visi = fig.add_subplot(n_subplots, 1, index)
    daily_bw_plot(vis=visi, df=temp, label=dept)

# COMMAND ----------

audience = "mothers_day"
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
  f.col("upc").alias("gtin_no"),
  f.col("familyTree.commodity.name").alias("commodity"),
  f.col("familyTree.subCommodity.name").alias("sub_commodity"),
  f.col("familyTree.primaryDepartment.name").alias("department"),
  f.col("familyTree.primaryDepartment.recapDepartment.name").alias("sub_department"),
  f.col("familyTree.primaryDepartment.recapDepartment.department.name").alias("micro_department"),
)
#
gcard_upcs = pim.filter(f.col("commodity") == "GREETING CARDS").\
select("gtin_no").\
dropDuplicates()

#For assigning each gtin_no their commodity and sub-commodity
pim = pim.select("commodity", "sub_commodity", "micro_department")
pim = pim.dropDuplicates(["commodity", "sub_commodity"])
pim.cache()

temp = pim.select(["commodity", "micro_department"]).dropDuplicates()
h_comms = h_comms.join(temp, "commodity", "left")
h_comms = h_comms.orderBy(["micro_department", "commodity"])
h_comms = h_comms.select("micro_department", "commodity", "seasonality_score")
del(temp)

h_scomms = h_scomms.join(pim, ["commodity", "sub_commodity"], "left")
h_scomms = h_scomms.orderBy(["micro_department", "commodity", "sub_commodity"])
h_scomms = h_scomms.select("micro_department", "commodity", "sub_commodity", "seasonality_sub_score")

h_comms.show(10, truncate=False)
h_scomms.show(10, truncate=False)

# COMMAND ----------

h_dates = {
  "super_bowl": "20230212",
  "valentines_day": "20230214",
  "st_patricks_day": "20230317",
  "easter": "20230409",
  "may_5th": "20230505",
  "mothers_day": "20230514",
  "memorial_day": "20230529",
  "fathers_day": "20230618",
  "july_4th": "20230704",
  "back_to_school": "20230819",
  "labor_day": "20230904",
  "halloween": "20231031",
  "thanksgiving": "20231123",
  "christmas_eve": "20231224",
  "new_years_eve": "20231231",
}
h_enddate = h_dates[audience]
h_enddate = datetime.strptime(h_enddate, '%Y%m%d')
h_startdate = h_enddate - timedelta(days=6)

acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(h_startdate, h_enddate, apply_golden_rules=golden_rules(['customer_exclusions']))
acds = acds.select("ehhn", "gtin_no")

gcard_trans = acds.join(gcard_upcs, "gtin_no", "inner")
gcard_ehhns = gcard_trans.select("ehhn").dropDuplicates()
gcard_ehhns = gcard_ehhns.withColumn("bought_greeting_card", f.lit("Yes"))

gcard_ehhns.cache()
print(f"Number of households that bought a greeting card during the holiday week: {gcard_ehhns.count()}")

# COMMAND ----------

#Read in datasets by holiday
output_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + audience + "/"

dept_fp = output_dir + "{}_microdepartment_spending".format(audience)
dept_df = spark.read.format("delta").load(dept_fp)

daily_fp = output_dir + "{}_daily_spending".format(audience)
daily_df = spark.read.format("delta").load(daily_fp)

ehhn_fp = output_dir + "{}_ehhn_spending".format(audience)
ehhn_df = spark.read.format("delta").load(ehhn_fp)
ehhn_df = ehhn_df.join(gcard_ehhns, "ehhn", "left")
ehhn_df = ehhn_df.fillna({"bought_greeting_card": "No"})

ehhn_df = ehhn_df.limit(10000)
dept_df = dept_df.join(ehhn_df.select("ehhn"), "ehhn", "inner")
daily_df = daily_df.join(ehhn_df.select("ehhn"), "ehhn", "inner")

ehhn_df.cache()
dept_df.cache()
daily_df.cache()

print("31 day spender count by whether or not they bought a greeting card:\n")
ehhn_df.\
groupby("bought_greeting_card").\
agg(f.count("ehhn").alias("ehhn_count")).\
show(50, truncate=False)

# COMMAND ----------

dbfs_dir1 = "/dbfs/dbfs/FileStore/users/c127414/"
fn = "visual_analysis_{}.pdf".format(audience)
pdf = PdfPages(dbfs_dir1 + fn)

# COMMAND ----------

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

#Figure 1 - Analyze dollars spent
#figure dimensions - (width x height)
fig1 = plt.figure(figsize=(15, 15))
n_subplots = 5
index = 1

#Sub-plot 1 -
vis1 = fig1.add_subplot(n_subplots, 1, index)
x_limit = max([x33, x66, cut_off])
kde_plot_by_group(vis=vis1, df=ehhn_df, num_col="dollars_spent", group_col="bought_greeting_card", x_limit=x_limit)
#Place cut-offs for 33rd/66th percentiles
vis1.axvline(x=x33, color='r', linestyle='--', linewidth=2)
vis1.axvline(x=x66, color='r', linestyle='--', linewidth=2)
#Place cut-off from clustering based method
vis1.axvline(x=cut_off, color='b', linestyle='--', linewidth=2)
vis1.legend()
del(x_limit)

index += 1
#Sub-plot 2 -
vis2 = fig1.add_subplot(n_subplots, 1, index)
kde_plot_by_group(vis=vis2, df=ehhn_df, num_col="dollars_spent", group_col="percentile_class", x_limit=x66)

index += 1
#Sub-plot 3 - 
vis3 = fig1.add_subplot(n_subplots, 1, index)
counts = ehhn_df.\
groupby("percentile_class", "bought_greeting_card").\
agg(
  f.count("percentile_class").alias("ehhn_count"),
)
table(df=counts, vis=vis3)

index += 1
#Sub-plot 4 -
vis4 = fig1.add_subplot(n_subplots, 1, index)
kde_plot_by_group(vis=vis4, df=ehhn_df, num_col="dollars_spent", group_col="cluster_class", x_limit=cut_off)

index += 1
#Sub-plot 5 - 
vis5 = fig1.add_subplot(n_subplots, 1, index)
counts = ehhn_df.\
groupby("cluster_class", "bought_greeting_card").\
agg(
  f.count("cluster_class").alias("ehhn_count"),
)
table(df=counts, vis=vis5)

#Adjust layout and save figure to specified PDF
plt.tight_layout()
pdf.savefig(fig1)
plt.show()

# COMMAND ----------

#Reset the dataframe
ehhn_df = ehhn_df.select("ehhn", "dollars_spent", "weighted_dollars_spent", "bought_greeting_card")
ehhn_df = ehhn_df.filter(f.col("weighted_dollars_spent") > 0)
del(x33, x66, cut_off)

#Do percentile clustering by WEIGHTED dollars spent
output = percentile_clustering(df=ehhn_df, num_col="weighted_dollars_spent")
ehhn_df = output[0]
percentile_df = ehhn_df
percentiles = output[1]
x33 = percentiles[0]
x66 = percentiles[1]
del(output, percentiles)

#Do kmean clustering by WEIGHTED dollars spent
output = kmean_clustering(df=ehhn_df, num_col="weighted_dollars_spent")
ehhn_df = output[0]
cluster_df = ehhn_df
cut_off = output[1]
del(output)

#Figure 2 - Analyze WEIGHTED dollars spent
#figure dimensions - (width x height)
fig1 = plt.figure(figsize=(15, 15))
n_subplots = 5
index = 1

#Sub-plot 1 -
vis1 = fig1.add_subplot(n_subplots, 1, index)
x_limit = max([x33, x66, cut_off])
kde_plot_by_group(vis=vis1, df=ehhn_df, num_col="weighted_dollars_spent", group_col="bought_greeting_card", x_limit=x_limit)
#Place cut-offs for 33rd/66th percentiles
vis1.axvline(x=x33, color='r', linestyle='--', linewidth=2)
vis1.axvline(x=x66, color='r', linestyle='--', linewidth=2)
#Place cut-off from clustering based method
vis1.axvline(x=cut_off, color='b', linestyle='--', linewidth=2)
vis1.legend()
del(x_limit)

index += 1
#Sub-plot 2 -
vis2 = fig1.add_subplot(n_subplots, 1, index)
kde_plot_by_group(vis=vis2, df=ehhn_df, num_col="weighted_dollars_spent", group_col="percentile_class", x_limit=x66)

index += 1
#Sub-plot 3 - 
vis3 = fig1.add_subplot(n_subplots, 1, index)
counts = ehhn_df.\
groupby("percentile_class", "bought_greeting_card").\
agg(
  f.count("percentile_class").alias("ehhn_count"),
)
table(df=counts, vis=vis3)

index += 1
#Sub-plot 4 -
vis4 = fig1.add_subplot(n_subplots, 1, index)
kde_plot_by_group(vis=vis4, df=ehhn_df, num_col="weighted_dollars_spent", group_col="cluster_class", x_limit=cut_off)

index += 1
#Sub-plot 5 - 
vis5 = fig1.add_subplot(n_subplots, 1, index)
counts = ehhn_df.\
groupby("cluster_class", "bought_greeting_card").\
agg(
  f.count("cluster_class").alias("ehhn_count"),
)
table(df=counts, vis=vis5)

#Adjust layout and save figure to specified PDF
plt.tight_layout()
pdf.savefig(fig1)
plt.show()


# COMMAND ----------

#Get all unique departments in dataset
departments = dept_df.select("micro_department").dropDuplicates().collect()
departments = [x["micro_department"] for x in departments]
fig1 = plt.figure(figsize=(15, 8*len(departments)))
#Visualize spending-across-time at overall and department levels
seasonal_plots(daily_df=daily_df, dept_df=dept_df, fig=fig1)

#Adjust layout and save figure to specified PDF
plt.subplots_adjust(hspace=3)
plt.tight_layout()
pdf.savefig(fig1)
plt.show()

# COMMAND ----------

# Create a figure and an axis
fig, ax = plt.subplots(figsize=(15, 15))

# Create the table
table(df=h_comms, vis=ax)

plt.tight_layout()
pdf.savefig(fig)
plt.show()

# COMMAND ----------

# Create a figure and an axis
fig, ax = plt.subplots(figsize=(15, 70))

# Create the table
table(df=h_scomms, vis=ax, font_size=15)

plt.tight_layout()
pdf.savefig(fig)
plt.show()

# COMMAND ----------

pdf.close()

dbfs_dir2 = "dbfs:/dbfs/FileStore/users/c127414/"
abfs_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"
dbutils.fs.cp(dbfs_dir2 + fn, abfs_dir + fn)

# COMMAND ----------

daily_df.limit(100).display()

# COMMAND ----------

import numpy as np
import pandas as pd

#Isolate to the day we'd like to freeze
df = daily_df.filter(f.col("date") == 20230508)

# Calculate the 25th and 75th quantiles
quantiles = df.approxQuantile("dollars_spent", [0.25, 0.75], 0.0)
q1 = quantiles[0]  # 25th quantile
q3 = quantiles[1]  # 75th quantile

# Calculate the interquartile range (IQR)
iqr = q3 - q1

# Calculate the upper and lower limits
lower_limit = q1 - 1.5 * iqr
upper_limit = q3 + 1.5 * iqr

# Show the results
print(f"25th quantile (Q1): {q1}")
print(f"75th quantile (Q3): {q3}")
print(f"Interquartile Range (IQR): {iqr}")
print(f"Lower Limit: {lower_limit}")
print(f"Upper Limit: {upper_limit}")

# COMMAND ----------

#Create the box-and-whiskers plot
df = daily_df.filter(f.col("date") == 20230508)
df = df.toPandas()
x = sns.boxplot(
    x='date', y='dollars_spent', data=df, showfliers=False,
    boxprops=dict(color='black', facecolor='white'),
    whiskerprops=dict(color='black'),
    capprops=dict(color='black'),
    medianprops=dict(color='black'),
    flierprops=dict(markerfacecolor='black', markeredgecolor='black'),
)
x

# COMMAND ----------

# Optional: Extracting the plotted data (whiskers)
whiskers = [line.get_ydata() for line in x.get_lines()[1:3]]
print(f"Whiskers: {whiskers}")

# COMMAND ----------

cutoffs = [x[0] for x in boxplot_stats]
cutoffs.sort()
q3 = cutoffs[3]
q3

# COMMAND ----------

#Q1 to lower bound
#Q3 to upper limit
#Lower Limit
#Upper Limit
#Median
boxplot_stats = [line.get_ydata() for line in x.get_lines()]
boxplot_upper_limit = max([i[0] for i in boxplot_stats])
print(boxplot_upper_limit)

# COMMAND ----------

date_cutoff = 20230508

quantiles = list(range(0, 100, 5))
quantiles = [i/100 for i in quantiles]
df = daily_df.filter(f.col("date") == date_cutoff)
quantile_cutoffs = df.approxQuantile("dollars_spent", quantiles, 0.0)
quantile_cutoffs



#date_cutoff = 20230508
#ehhns = daily_df.filter(f.col("date") > date_cutoff)
#ehhns = ehhns.filter(f.col("dollars_spent") > q3)
#ehhns = ehhns.select("ehhn").dropDuplicates()
#ehhns.count()

# COMMAND ----------

counts = []
gcard_confirms = []

for q in quantile_cutoffs:
  audience = daily_df.filter(f.col("date") > date_cutoff)
  audience = audience.filter(f.col("dollars_spent") > q)
  audience = audience.select("ehhn").dropDuplicates()
  counts += [audience.count()]

  gcard_confirms += [audience.join(gcard_ehhns, "ehhn", "inner").count()]

# COMMAND ----------

data = [quantiles, quantile_cutoffs, counts, gcard_confirms]
quantile_stats = pd.DataFrame(data)
quantile_stats = quantile_stats.T
quantile_stats.columns = ["quantile", "quantile_cutoff", "ehhn_count", "purchased_card_count"]
quantile_stats["ehhn_count"] = quantile_stats["ehhn_count"].astype(int)
quantile_stats["purchased_card_count"] = quantile_stats["purchased_card_count"].astype(int)

quantile_stats["card_purchasers_captured"] = (quantile_stats["purchased_card_count"]/gcard_ehhns.count())*100
quantile_stats["card_purchasers_captured"] = quantile_stats["card_purchasers_captured"].round(2)
quantile_stats

# COMMAND ----------

import matplotlib.pyplot as plt

# Assuming you have a DataFrame named df
plt.figure(figsize=(8, 8))
plt.scatter(quantile_stats['quantile'], quantile_stats['card_purchasers_captured'], color='blue', marker='o')
plt.gca().invert_xaxis()

# Add labels and a title
plt.xlabel('Quantile')
plt.ylabel('Card Purchasers Captured')
plt.title('Scatterplot of Quantile vs. Card Purchasers Captured')

# Show the plot
plt.show()

# COMMAND ----------

def daily_bw_plot(vis, df, label):
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
  #dates = pandas_df["date"]
  #vis.set_xticks(dates)
  #plt.setp(vis.get_xticklabels(), rotation=45, ha="right")

# COMMAND ----------

def daily_bw_plot(vis, df, label):
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
# Aggregate data
  aggregated_df = df.\
    groupBy("date").\
    agg(f.countDistinct("ehhn").alias("ehhn_count"))
  #Convert to Pandas DataFrame
  pandas_df = aggregated_df.toPandas()
  pandas_df = pandas_df.sort_values(by=["date"], ascending=True)
  pandas_df = pandas_df.reset_index(drop=True)
  #Rename columns for clarity
  #exploded_df.columns = ['date', 'dollars_spent']
  #Formate the date column for readability
  pandas_df['date'] = pandas_df.astype(str)
  pandas_df['date'] = pandas_df['date'].apply(lambda x: f"{x[:4]}-{x[4:6]}-{x[6:]}")

  #Create the box-and-whiskers plot
  sns.lineplot(x='date', y='ehhn_count', data=pandas_df, marker="o")

  vis.set_title('EHHN Count Across Time ({})'.format(label))
  vis.set_xlabel('Date')
  vis.set_xticks(vis.get_xticks())
  vis.set_xticklabels(vis.get_xticklabels(), rotation=60)
  vis.set_ylabel('EHHN Count')
 
def seasonal_plots(daily_df, dept_df, fig):
  """
  """
  #Get all unique departments in dataset
  departments = dept_df.select("micro_department").dropDuplicates().collect()
  departments = [x["micro_department"] for x in departments]

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

# COMMAND ----------

#Will hand over to Sales + friends: commodities, sub-commodities chosen,
#Spending plots + ehhn counts per department + overall
#Get all unique departments in dataset
departments = dept_df.select("micro_department").dropDuplicates().collect()
departments = [x["micro_department"] for x in departments]
fig1 = plt.figure(figsize=(15, 20*len(departments)))
#Visualize spending-across-time at overall and department levels
seasonal_plots(daily_df=daily_df, dept_df=dept_df, fig=fig1)

#Adjust layout and save figure to specified PDF
plt.subplots_adjust(hspace=0.5)
#plt.tight_layout()
#pdf.savefig(fig1)
#plt.show()

# COMMAND ----------

#Pull in multivariate spending distribution for all 31 days.

#For each day, calculate mean matrix and covariance
#(variables is spending in each micro-department)

#For the days within the lookback period, calculate the
#average mean matrix and calculate the average covariance
#matrix.

#For the days outside the lookback period, calculate the
#average mean matrix and calculate the average covariance
#matrix.

#Now you have two distributions: the normal state and the holiday state.

#For each ehhn that purchased in the lookback window, measure if they
#are closer to the normal state or the holiday state.

#*Will need to throw out spending departments that did not have data 
#on the required days. We have millions of observations. If a micro-department
#does not have data on that day then we should drop it.


# COMMAND ----------


