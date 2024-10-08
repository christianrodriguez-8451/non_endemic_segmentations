# Databricks notebook source
"""
Demonstrates the methodology in seasonal_outlier_detection.py.
This code is not necessary in the production process. Its main
purpose is as an illustrative example for somebody trying to
learn the methodology.

GREETING CARDS is the sub-commodity used for this example. The code
can be adjusted to analyze other commdities/sub-commodities such as
PUMPKINS, ARRANGEMENTS, and ***. The methodology can be summarized 
as follows:

  1) Pull in the sub-commodity's time series of weekly units sold
  across time.
  2) Conduct STL (Seasonal Trend Decomposition using LOESS) to
  decompose the time series into its trend, seasonal, and residual
  components.
  3) Estimate the average and standard deviation of the
  seasonal values.
  4) Flag as outliers any seasonal values that have a
  value >= (Mean + 1.645 STD). This is the 95th percentile
  cutoff in a normal distribution.
  5) If those outlier values occured on a holiday week,
  then the sub-commodity is considered part of the holiday's
  product set for that given holiday.
"""

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import pyspark.sql.functions as f
from statsmodels.tsa.seasonal import STL
import numpy as np
import scipy.stats as stats

#Load the history data
fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + "seasonal_subcommodity_history"
df = spark.read.format("delta").load(fp)

#You can also do this at the commodity level too!
#fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + "seasonal_commodity_history"
#df = spark.read.format("delta").load(fp)

# COMMAND ----------

#Potential Z-Scores to use as cut-offs.
#DSR used 1.645 as the cuf-off.
percentages = range(90, 100)
for percentile in percentages:
    z_score = stats.norm.ppf(percentile / 100)
    print(f"Cut-off for {percentile}% coverage: {z_score:.3f} standard deviations from the mean")

# COMMAND ----------

#FOR ILLUSTRAIVE EXAMPLE - Let's assume the time series
#is for the CARDS SEASONAL sub-commodity. The sub-
#commodity belongs to the GREETING CARDS commodity.
#We are going to take the time series of (week vs units
#sold) and conduct STL (Seasonal Trend Decomposition using LOESS).
#STL is applied to extract the seasonal values of the time series.
#Since these values have been de-trended, the mean and standard
#deviation of these seasonal values are calculated to impose
#Z-score cut-off. DSR settled on a Z-score cut-off of (mean + 1.645 STD)
# That is equivalent to a p-value cut-off of 95% if we assume the
# distribution is normal. If the sub-commodity has any seasonal values greater than
#***, them the sub-commodity spiked for that week.

#Isolate the sub-commodity data to just CARDS SEASONAL.
#You can do this for other examples, just change my_column
#and my_value.
my_column = "sub_commodity"
my_value = "CARDS SEASONAL"
sub_df = df.filter(f.col(my_column) == my_value)
sub_df = sub_df.toPandas()
sub_df = sub_df.sort_values(by="week", ascending=True)
sub_df = sub_df.reset_index(drop=False)

# Assuming 'data' is your time series data with 'week' as the index
# 'units_sold' is the value column

# Perform STL decomposition
stl = STL(sub_df['units_sold'], seasonal=3, period=13)  # Assuming monthly seasonality, adjust 'seasonal' parameter as needed
result = stl.fit()

# Plot the original time series data
plt.figure(figsize=(10, 6))
plt.subplot(4, 1, 1)
plt.plot(sub_df.index, sub_df['units_sold'], label='Original Data')
plt.title('Original Time Series Data')
plt.xlabel('Date')
plt.ylabel('Units Sold')
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

#Plot the distribution of seasonal values.
#They are definitely not perfectly normal,
#but DSR concluded they are normal enough!
plt.figure(figsize=(8, 6))
plt.hist(result.seasonal, bins=30, color='blue', alpha=0.7)
plt.title('Distribution of Seasonal Values')
plt.xlabel('Seasonal Values')
plt.ylabel('Frequency')
plt.grid(True)

plt.tight_layout()
plt.show()

# Calculate the mean and standard deviation of the seasonal values
mean_seasonal = np.mean(result.seasonal)
std_seasonal = np.std(result.seasonal)
# Define the threshold as values greater than or equal to 1.645 standard deviations from the mean
#1.645 is the cut-off for 95% and above
threshold = mean_seasonal + 1.645 * std_seasonal

# Identify outliers based on the threshold and pull what week they spiked
outliers = result.seasonal[result.seasonal > threshold]
outliers_weeks = list(outliers.index)

# COMMAND ----------

#FOR ILLUSTRATIVE EXAMPLE - These are all the weeks where greeting cards
#spiked.
list(outliers.index)

# COMMAND ----------

#FOR ILLUSTRATIVE EXAMPLE - We have all the weeks where greeting cards spiked.
#Do these any of these spikes land on a holiday? Yes, all of them!
my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"
fn = "holidays"
fp = my_dir + fn
holidays = spark.read.format("delta").load(fp)
holidays = holidays.select("week", "holiday")
holidays = holidays.dropDuplicates()
holidays = holidays.filter(f.col("week").isin(list(outliers.index)))
holidays.orderBy(["week"]).show(50, truncate=False)
