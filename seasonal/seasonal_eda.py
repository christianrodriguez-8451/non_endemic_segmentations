# Databricks notebook source
import pyspark.sql.functions as f
import os


my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"
fn = "timeline"
fp = my_dir + fn
df = spark.read.format("delta").load(fp)

#my_subcomms = ["CHRISTMAS", "HALLOWEEN", "EASTER", "HAMS-SPIRAL", "POTATOES GOLD (BULK&BAG)"]
#df = df.filter(f.col("sub_commodity").isin(my_subcomms))

def write_out(df, fp, delim="^", fmt="csv"):
  #Placeholder filepath for intermediate processing
  temp_target = os.path.dirname(fp) + "/" + "temp"

  #Write out df as partitioned file. Write out ^ delimited
  df.coalesce(1).write.options(header=True, delimiter=delim).mode("overwrite").format(fmt).save(temp_target)

  #Copy desired file from parititioned directory to desired location
  temporary_fp = os.path.join(temp_target, dbutils.fs.ls(temp_target)[3][1])
  dbutils.fs.cp(temporary_fp, fp)
  dbutils.fs.rm(temp_target, recurse=True)

fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/timeline.csv"
write_out(df, fp, delim=",", fmt="csv")

# COMMAND ----------

import pyspark.sql.functions as f

my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"
fn = "raw_weekly_history"
fp = my_dir + fn
df = spark.read.format("delta").load(fp)

window_sizes = [4, 13, 26, 52]
for w in window_sizes:
  fp = my_dir + "rolling_{}week_history".format(w)
  roll_df = spark.read.format("delta").load(fp)
  roll_df = roll_df.withColumn("week", f.col("week") - 1)

  df = df.join(roll_df, ["week", "sub_commodity"], "inner")
  df = df.withColumn("{}w_share_rank_diff".format(w), f.col("{}w_share_rank".format(w)) - f.col("share_rank"))

df = df.select(
  "week", "sub_commodity", "share_rank",
  "4w_share_rank_diff", "13w_share_rank_diff", "26w_share_rank_diff", "52w_share_rank_diff",
)
df = df.orderBy(["week", "share_rank"])
df.show(50, truncate=False)

# COMMAND ----------

#Bring in holidays
fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/holidays"
holidays = spark.read.format("delta").load(fp)
holidays.groupBy("holiday").agg(f.sort_array(f.collect_set("week")).alias("holiday_weeks")).show(50, truncate=False)


# COMMAND ----------

holi_df = df.join(holidays.select("week", "holiday").dropDuplicates(), "week", "inner")

my_holi = "christmas_eve"
my_week = 207
holi_df = holi_df.filter(f.col("holiday") == my_holi)
holi_df = holi_df.filter(f.col("week") == my_week)
holi_df.show(50, truncate=False)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Convert PySpark DataFrame to Pandas DataFrame
keep_cols = ["sub_commodity", "4w_share_rank_diff", "13w_share_rank_diff", "26w_share_rank_diff", "52w_share_rank_diff"]
pandas_df = holi_df.select(keep_cols).toPandas()

# Create a scatter plot matrix using Seaborn
sns.set(style="ticks")
scatter_matrix = sns.pairplot(pandas_df)
plt.show()

# COMMAND ----------

from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import pandas as pd


# Extracting the numeric columns
numeric_columns = keep_cols
data = pandas_df[numeric_columns]

# Standardize the data
scaler = StandardScaler()
data_standardized = scaler.fit_transform(data)

# Perform PCA
pca = PCA(n_components=2)
principal_components = pca.fit_transform(data_standardized)

# Create a new DataFrame with the principal components
pc_df = pd.DataFrame(data=principal_components, columns=["PC1", "PC2"])

# Concatenate the principal components with the original DataFrame
final_df = pd.concat([pc_df, pandas_df], axis=1)

sns.scatterplot(data=final_df, x="PC1", y="PC2")
plt.title("Scatter Plot of First Two Principal Components")
plt.show()

# COMMAND ----------

from sklearn.cluster import KMeans

# Assuming 'pandas_df' is your Pandas DataFrame with numeric columns "x", "y", "z"
# You may need to replace these column names with the actual column names in your DataFrame

# Extracting the numeric columns
#numeric_columns = ["4w_share_rank_diff", "13w_share_rank_diff", "26w_share_rank_diff", "52w_share_rank_diff"]
#numeric_columns = ["13w_share_rank_diff", "52w_share_rank_diff"]
#numeric_columns = ["4w_share_rank_diff", "13w_share_rank_diff", "52w_share_rank_diff"]
numeric_columns = ["52w_share_rank_diff"]
data = pandas_df[numeric_columns]

# Standardize the data
scaler = StandardScaler()
data_standardized = scaler.fit_transform(data)

# Perform K-Means clustering
kmeans = KMeans(n_clusters=3, random_state=42)
pandas_df['cluster'] = kmeans.fit_predict(data_standardized)

# Visualize the clusters using a scatter plot
scatter_matrix = sns.pairplot(data=pandas_df, hue="cluster", palette="viridis")
#sns.scatterplot(data=pandas_df, x="x", y="y", hue="cluster", palette="viridis")
plt.show()

# COMMAND ----------

cluster = pandas_df.loc[pandas_df["cluster"] == 0, :]
cluster = cluster.reset_index(drop=True)
slice_i = 3
cluster.iloc[list(range(50*slice_i, 50*slice_i + 50 + 1)), :].head(50)

# COMMAND ----------

cluster.shape

# COMMAND ----------

cluster.head(50)

# COMMAND ----------

#Add more metrics!
#Add number of houses that bought the basket
#Add number of units sold
my_week

# COMMAND ----------

string = '{"name": "Grilling Enthusiast", "description": "Buyers who have a high affinity for purchasing grilling related products including charcoal, grills, grill brushes and grilling accessories.", "cells": [{"type": "PRE_DEFINED_SEGMENTATION", "predefinedSegment": {"segmentationId": "a34b1cd2-5bcf-cbd8-cabf-8602d937ac14", "selectedValues": ["H", "M], "segmentationName": "SUMMER_BBQ"}, "order": 0}], "combineOperator": "AND", "krogerSegment": true, "id": "f31c69ae-a0e2-35ae-632a-b459162edf87"}'
string[340:348]

# COMMAND ----------

#Look inton time series with exponential smoothing
