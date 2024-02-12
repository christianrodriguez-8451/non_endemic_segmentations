# Databricks notebook source
import pandas as pd
import matplotlib.pyplot as plt
import pyspark.sql.functions as f
from statsmodels.tsa.seasonal import STL
import numpy as np
import scipy.stats as stats

#Load the history data
my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"
fn = "raw_weekly_history"
fp = my_dir + fn
df = spark.read.format("delta").load(fp)

df = df.orderBy(["sub_commodity", "week"])
df.show(50, truncate=False)

# COMMAND ----------

percentages = range(90, 100)
for percentile in percentages:
    z_score = stats.norm.ppf(percentile / 100)
    print(f"Cut-off for {percentile}% coverage: {z_score:.3f} standard deviations from the mean")

# COMMAND ----------

sub_comm = "PUMPKINS"
sub_df = df.filter(f.col("sub_commodity") == sub_comm)
sub_df = sub_df.toPandas()
sub_df = sub_df.sort_values(by="week", ascending=True)
sub_df = sub_df.reset_index(drop=False)

# Assuming 'data' is your time series data with 'week' as the index
# 'dollars_spent' is the value column

# Perform STL decomposition
stl = STL(sub_df['dollars_spent'], seasonal=3, period=13)  # Assuming monthly seasonality, adjust 'seasonal' parameter as needed
result = stl.fit()

# Plot the original time series data
plt.figure(figsize=(10, 6))
plt.subplot(4, 1, 1)
plt.plot(sub_df.index, sub_df['dollars_spent'], label='Original Data')
plt.title('Original Time Series Data')
plt.xlabel('Date')
plt.ylabel('Dollars Spent')
plt.legend()

# Plot the trend component
plt.subplot(4, 1, 2)
plt.plot(sub_df.index, result.trend, label='Trend')
plt.title('Trend Component')
plt.xlabel('Date')
plt.ylabel('Trend')
plt.legend()

# Plot the seasonal component
plt.subplot(4, 1, 3)
plt.plot(sub_df.index, result.seasonal, label='Seasonal')
plt.title('Seasonal Component')
plt.xlabel('Date')
plt.ylabel('Seasonal')
plt.legend()

# Plot the residual component
plt.subplot(4, 1, 4)
plt.plot(sub_df.index, result.resid, label='Residual')
plt.title('Residual Component')
plt.xlabel('Date')
plt.ylabel('Residual')
plt.legend()

plt.figure(figsize=(8, 6))
plt.hist(result.seasonal, bins=30, color='blue', alpha=0.7)
plt.title('Distribution of Seasonal Values')
plt.xlabel('Seasonal Values')
plt.ylabel('Frequency')
plt.grid(True)
#plt.show()

plt.tight_layout()
plt.show()

# Calculate the mean and standard deviation of the seasonal values
mean_seasonal = np.mean(result.seasonal)
std_seasonal = np.std(result.seasonal)
# Define the threshold as values greater than or equal to 1.881 standard deviations from the mean
#1.881 is the cut-off for 97% and above
threshold = mean_seasonal + 1.645 * std_seasonal

# Identify outliers based on the threshold and pull what week they pulled
outliers = result.seasonal[result.seasonal > threshold]
outliers_weeks = list(outliers.index)

# COMMAND ----------

list(outliers.index)

# COMMAND ----------

subcomms_outlier_weeks = []
all_sub_comms = df.select("sub_commodity").distinct().rdd.map(lambda row: row[0]).collect()
all_sub_comms.sort()
for sub_comm in all_sub_comms:
  sub_df = df.filter(f.col("sub_commodity") == sub_comm)
  sub_df = sub_df.toPandas()
  sub_df = sub_df.sort_values(by="week", ascending=True)
  sub_df = sub_df.reset_index(drop=False)

  # Perform STL decomposition
  stl = STL(sub_df['dollars_spent'], seasonal=3, period=13)  # Assuming monthly seasonality, adjust 'seasonal' parameter as needed
  result = stl.fit()

  # Calculate the mean and standard deviation of the seasonal values
  mean_seasonal = np.mean(result.seasonal)
  std_seasonal = np.std(result.seasonal)
  # Define the threshold as values greater than or equal to 1.881 standard deviations from the mean
  #1.881 is the cut-off for 97% and above
  p = 1.282
  threshold = mean_seasonal + p * std_seasonal

  # Identify outliers based on the threshold and pull what week they pulled
  outliers = result.seasonal[result.seasonal > threshold]
  outliers_weeks = list(outliers.index)

  if len(outliers_weeks) != 0:
    sub_comm = [sub_comm]*len(outliers_weeks)
    append_df = pd.DataFrame({"sub_commodity": sub_comm, "week": outliers_weeks})
    subcomms_outlier_weeks += [append_df]

subcomms_outlier_weeks = pd.concat(subcomms_outlier_weeks, ignore_index=True)
subcomms_outlier_weeks = spark.createDataFrame(subcomms_outlier_weeks)
subcomms_outlier_weeks.show(50, truncate=False)

# COMMAND ----------

my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"
fn = "holidays"
fp = my_dir + fn
holidays = spark.read.format("delta").load(fp)
holidays = holidays.select("week", "holiday")
holidays = holidays.dropDuplicates()

fn = "timeline"
fp = my_dir + fn
timeline = spark.read.format("delta").load(fp)
timeline = timeline.select("week", "year")

holidays = holidays.join(timeline, "week", "inner")
holidays = holidays.dropDuplicates()
holiday_subcomms = subcomms_outlier_weeks.join(holidays, "week", "inner")
holiday_subcomms.show(50, truncate=False)

# COMMAND ----------

holiday_subcomms = holiday_subcomms.filter(f.col("year") >= 2021)
holiday_subcomms = holiday_subcomms.groupBy("sub_commodity", "holiday").agg(f.countDistinct("year").alias("unique_year_count"))
holiday_subcomms = holiday_subcomms.filter(f.col("unique_year_count") >= 2)
holiday_subcomms = holiday_subcomms.orderBy(["holiday", "unique_year_count", "sub_commodity"], ascending=[True, False, True])
holiday_subcomms.show(50, truncate=False)

# COMMAND ----------

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

write_out(holiday_subcomms, my_dir+"holiday_sub_commodities_90.csv")

# COMMAND ----------


