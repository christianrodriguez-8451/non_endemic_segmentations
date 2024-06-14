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

from effodata import ACDS, golden_rules
import resources.config as config
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

# Initialize a Spark session
spark = SparkSession.builder.appName("seasonal_spending").getOrCreate()
#def daily_plots():
def daily_bw_plot(df, label):
# Aggregate data
  aggregated_df = df.\
    groupBy("date").\
    agg(f.collect_list("dollars_spent").alias("dollars_spent_list"))
  # Convert to Pandas DataFrame
  pandas_df = aggregated_df.toPandas()
  # Explode the list column to separate rows
  exploded_df = pandas_df.explode('dollars_spent_list')
  # Rename columns for clarity
  exploded_df.columns = ['date', 'dollars_spent']

  # Create the box-and-whiskers plot
  plt.figure(figsize=(12, 6))
  sns.boxplot(x='date', y='dollars_spent', data=exploded_df, showfliers=False)
  plt.title('Box-and-Whiskers Plot of Dollars Spent Across Time ({})'.format(label))
  plt.xlabel('Date')
  plt.ylabel('Dollars Spent')
  plt.xticks(rotation=45)
  plt.show()

def seasonal_plots(daily_df, dept_df):
  """
  """
  daily_bw_plot(daily_df, "OVERALL")

  #Get all unique departments in dataset
  departments = dept_df.select("department").dropDuplicates().collect()
  departments = [x["department"] for x in departments]
  for dept in departments:
    temp = dept_df.filter(f.col("department") == dept)
    daily_bw_plot(temp, dept)

# COMMAND ----------

audience = "memorial_day"
output_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + audience + "/"
dept_fp = output_dir + "{}_department_spending".format(audience)
daily_fp = output_dir + "{}_daily_spending".format(audience)
ehhn_fp = output_dir + "{}_ehhn_spending".format(audience)

dept_df = spark.read.format("delta").load(dept_fp)
daily_df = spark.read.format("delta").load(daily_fp)
ehhn_df = spark.read.format("delta").load(ehhn_fp)

# COMMAND ----------

dept_df.show(1, truncate=False)

# COMMAND ----------

seasonal_plots(daily_df, dept_df)

# COMMAND ----------

#Calculate 33rd and 66th percentile cut-offs
percentiles = ehhn_df.approxQuantile("dollars_spent", [0.33, 0.66], 0.01)
x33 = percentiles[0]
x66 = percentiles[1]

pandas_df = ehhn_df.toPandas()
data = pandas_df[["dollars_spent"]]

# Standardize the data
scaler = StandardScaler()
data_standardized = scaler.fit_transform(data)
# Perform K-Means clustering
kmeans = KMeans(n_clusters=2, random_state=42)
pandas_df['cluster'] = kmeans.fit_predict(data_standardized)

# Convert Pandas DataFrame to PySpark DataFrame
spark_df = spark.createDataFrame(pandas_df)
cluster_df = spark_df.groupBy("cluster").agg(
  f.min("dollars_spent").alias("min_dollars_spent"),
  f.avg("dollars_spent").alias("avg_dollars_spent"),
  f.max("dollars_spent").alias("max_dollars_spent"),
  f.count("ehhn").alias("ehhn_count"),
)
cluster_df.show(5, truncate=False)
cut_off = cluster_df.agg(f.max("min_dollars_spent").alias("cut_off")).collect()[0]["cut_off"]

# COMMAND ----------

from scipy.stats import gaussian_kde
import numpy as np

# Collect data from Spark DataFrame
data_collected = ehhn_df.select("dollars_spent").rdd.flatMap(lambda x: x).collect()
# Compute the kernel density estimate
kde = gaussian_kde(data_collected)
# Create a range of values
x_range = np.linspace(min(data_collected), max(data_collected), 10000)
# Evaluate the KDE over the range of values
kde_values = kde(x_range)
# Extract 'dollars_spent' column and convert to Pandas DataFrame
#temp = ehhn_df.select("dollars_spent").toPandas()

plt.figure(figsize=(10, 6))
plt.plot(x_range, kde_values, label='KDE')
plt.fill_between(x_range, kde_values, alpha=0.5)
plt.xlabel('Value')
plt.ylabel('Density')
plt.title('Kernel Density Estimate')
#Place cut-offs for 33rd/66th percentiles
plt.axvline(x=x33, color='r', linestyle='--', linewidth=2)
plt.axvline(x=x66, color='r', linestyle='--', linewidth=2)
#Place cut-off from clustering based method
plt.axvline(x=cut_off, color='b', linestyle='--', linewidth=2)
plt.xlim(0, cut_off*3)
plt.legend()
plt.show()

# COMMAND ----------

# Plot the KDE
plt.figure(figsize=(10, 6))
sns.histplot(temp['dollars_spent'])
#sns.kdeplot(temp['dollars_spent'], shade=True)
#Place cut-offs for 33rd/66th percentiles
plt.axvline(x=x33, color='r', linestyle='--', linewidth=2)
plt.axvline(x=x66, color='r', linestyle='--', linewidth=2)
#Place cut-off from clustering based method
plt.axvline(x=cut_off, color='b', linestyle='--', linewidth=2)
plt.xlim(0, cut_off*2)
plt.show()

# COMMAND ----------

#Plot dual plot of distribution plot for doing both traditional HML based on 33rd/66th
#and cluster
spark_df = spark_df.withColumn(
  "percentile_class",
  f.when(f.col("dollars_spent") < x33, "L")
        .otherwise(f.when(f.col("dollars_spent") > x66, "H")
                  .otherwise("M"))
)
spark_df = spark_df.withColumn(
  "cluster_class",
  f.when(f.col("dollars_spent") < cut_off, "REGULAR")
        .otherwise("HOLIDAY"))
spark_df.show(10, truncate=False)

# COMMAND ----------

from scipy.stats import gaussian_kde
import numpy as np

df = spark_df.limit(10000).toPandas()

# Step 2: Group by 'cluster' column
groups = df.groupby('cluster_class')

# Step 3: Define a color palette
colors = plt.cm.viridis(np.linspace(0, 1, len(groups)))

# Step 4: Set up the plot
plt.figure(figsize=(10, 6))

# Step 5: Loop through each group and corresponding color
for (name, group), color in zip(groups, colors):
    # Compute the KDE for the current group
    kde = gaussian_kde(group['dollars_spent'])
    
    # Create a range of values
    x_range = np.linspace(group['dollars_spent'].min(), group['dollars_spent'].max(), 10000)
    
    # Evaluate the KDE over the range of values
    kde_values = kde(x_range)
    
    # Plot the KDE with shading and color
    plt.plot(x_range, kde_values, label=f'Cluster {name}', color=color)
    plt.fill_between(x_range, kde_values, alpha=0.3, color=color)

# Step 6: Add labels and title
plt.xlabel('Value')
plt.ylabel('Density')
plt.title('Kernel Density Estimate by Cluster')
plt.legend()
plt.show()
