# Databricks notebook source
"""
This notebook is ran after seasonal_history.py.

Applies the methodology described in seasonal_outlier_detection_poc.py
to every commodity in seasonal_commodity_history and to every
sub-commodity in seasonal_subcommodity_history. For every year 
that the given commodity (or sub-commodity) spikes on a particular
holiday, assign that commodity a seasonality score for the given
holiday. The point schema is as follows:

  +2 if the commodity spikes in 2023. This is to prioritize recency.

  +1 if the commodity spikes in 2022.

  +1 if the commodity spikes in 2021.

Two files are outputted:

  holiday_commodities.csv := CSV file that contains every
  commodity that spiked at least once in the past 3 years
  for any of the given holidays.

  holiday_subcommodities.csv := CSV file that contains every
  sub-commodity that spiked at least once in the past 3 years
  for any of the given holidays.

These two CSV files are put into an Excel workbook. DSR reviews
this workbook one holiday at a time to determine which
commodities/sub-commodities belong in each holiday. Once the workbook
has been reviewed and finalized, turn the workbook into a CSV.
The CSV expected format is described in seasonal_spending.py.

To aid DSR's choices, output seasonal_commodity_history as a CSV
and create a pivot chart (line plot) out of it. Add slicers for
commodity/sub-commodity and you can visualize multiple time
series simultaneously. Assign the pivot fields
as follows:

  commodity -> Filters

  sub_commodity -> Legends(Series)

  week -> Axis ->(Categories)

  units_sold -> Values
"""

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import pyspark.sql.functions as f
from statsmodels.tsa.seasonal import STL
import numpy as np
import scipy.stats as stats

# COMMAND ----------

#Read in the time series of each commodity
fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + "seasonal_commodity_history"
df = spark.read.format("delta").load(fp)

comms = df.select("commodity").dropDuplicates()
comms = comms.orderBy(["commodity"])
comms = comms.select("commodity").collect()
comms = [x["commodity"] for x in comms]

#List that holds each week that the given commodity
#spikes in.
comms_outlier_weeks = []
for comm in comms:
  #Isolate the data to the given commodity and prep for STL
  sub_df = df.filter(f.col("commodity") == comm)
  sub_df = sub_df.toPandas()
  sub_df = sub_df.sort_values(by="week", ascending=True)
  sub_df = sub_df.reset_index(drop=False)

  #Perform STL decomposition - we could done a better job tuning the below
  #parameters.
  #Assuming monthly seasonality, adjust 'seasonal' parameter as needed
  stl = STL(sub_df['units_sold'], seasonal=3, period=13)  
  result = stl.fit()

  #Calculate the mean and standard deviation of the seasonal values
  mean_seasonal = np.mean(result.seasonal)
  std_seasonal = np.std(result.seasonal)
  #Define the threshold as values greater than or equal to 1.645 standard deviations from the mean
  #1.645 is the cut-off for 95%.
  p = 1.645
  threshold = mean_seasonal + p * std_seasonal

  #Identify outliers based on the threshold and pull what week they spiked
  outliers = result.seasonal[result.seasonal >= threshold]
  outliers_weeks = list(outliers.index)

  #If there was a spike for at least one week, add those weeks to the
  #comms_outlier_weeks container.
  if len(outliers_weeks) != 0:
    comm = [comm]*len(outliers_weeks)
    append_df = pd.DataFrame({"commodity": comm, "week": outliers_weeks})
    comms_outlier_weeks += [append_df]

#Append all the results together
comms_outlier_weeks = pd.concat(comms_outlier_weeks, ignore_index=True)
comms_outlier_weeks = spark.createDataFrame(comms_outlier_weeks)
comms_outlier_weeks.show(50, truncate=False)

# COMMAND ----------

#Read in the time series of each commodity
fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + "seasonal_subcommodity_history"
df = spark.read.format("delta").load(fp)

all_subcomms = df.select("commodity", "sub_commodity").dropDuplicates()
all_subcomms = all_subcomms.orderBy(["commodity", "sub_commodity"])
comms = all_subcomms.select("commodity").collect()
comms = [x["commodity"] for x in comms]
sub_comms = all_subcomms.select("sub_commodity").collect()
sub_comms = [x["sub_commodity"] for x in sub_comms]

#List that holds each week that the given sub-commodity
#spikes in.
subcomms_outlier_weeks = []
for comm, sub_comm in zip(comms, sub_comms):
  #Isolate the data to the given commodity and prep for STL
  sub_df = df.filter(f.col("commodity") == comm)
  sub_df = sub_df.filter(f.col("sub_commodity") == sub_comm)
  sub_df = sub_df.toPandas()
  sub_df = sub_df.sort_values(by="week", ascending=True)
  sub_df = sub_df.reset_index(drop=False)

  #Perform STL decomposition - we could done a better job tuning the below
  #parameters.
  #Assuming monthly seasonality, adjust 'seasonal' parameter as needed
  stl = STL(sub_df['units_sold'], seasonal=3, period=13)
  result = stl.fit()

  #Calculate the mean and standard deviation of the seasonal values.
  mean_seasonal = np.mean(result.seasonal)
  std_seasonal = np.std(result.seasonal)
  #Define the threshold as values greater than or equal to 1.645 standard deviations from the mean
  #1.645 is the cut-off for 95%.
  p = 1.645
  threshold = mean_seasonal + p * std_seasonal

  ##Identify outliers based on the threshold and pull what week they spiked.
  outliers = result.seasonal[result.seasonal >= threshold]
  outliers_weeks = list(outliers.index)

  #If there was a spike for at least one week, add those weeks to the
  #comms_outlier_weeks container.
  if len(outliers_weeks) != 0:
    comm = [comm]*len(outliers_weeks)
    sub_comm = [sub_comm]*len(outliers_weeks)
    append_df = pd.DataFrame({"commodity": comm, "sub_commodity": sub_comm, "week": outliers_weeks})
    subcomms_outlier_weeks += [append_df]

#Append all the results together
subcomms_outlier_weeks = pd.concat(subcomms_outlier_weeks, ignore_index=True)
subcomms_outlier_weeks = spark.createDataFrame(subcomms_outlier_weeks)
subcomms_outlier_weeks.show(50, truncate=False)

# COMMAND ----------

my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"

#Read in holidays to assign holiday to the relevant weeks
fn = "holidays"
fp = my_dir + fn
holidays = spark.read.format("delta").load(fp)
holidays = holidays.select("week", "holiday")
holidays = holidays.dropDuplicates()

#Read in timeline to assign year for each given week
fn = "timeline"
fp = my_dir + fn
timeline = spark.read.format("delta").load(fp)
timeline = timeline.select("week", "year")
holidays = holidays.join(timeline, "week", "inner")
holidays = holidays.dropDuplicates()

# COMMAND ----------

#Assign holiday to the commodity spikes
holiday_comms = comms_outlier_weeks.join(holidays, "week", "inner")
#Give more weight to commodities spiking in the latest year
holiday_comms = holiday_comms.\
  withColumn("score", f.when(holiday_comms["year"] == 2023, 2).otherwise(1))
#Only keep unique year occurences
holiday_comms = holiday_comms.dropDuplicates(["holiday", "commodity", "year"])
#Only keep 2021, 2022, 2023 data
holiday_comms = holiday_comms.filter(f.col("year") >= 2021)
#Create seasonality score based on 3 years of data
holiday_comms = holiday_comms.\
groupBy("commodity", "holiday").\
agg(f.sum("score").alias("seasonality_score"))
#At holiday/commodity level, order by score to understand most important commodities
holiday_comms = holiday_comms.orderBy(["holiday", "seasonality_score", "commodity"],
                                      ascending=[True, False, True])
holiday_comms.show(50, truncate=False)

# COMMAND ----------

#Assign holiday to the sub-commodity spikes
holiday_subcomms = subcomms_outlier_weeks.join(holidays, "week", "inner")
#Give more weight to commodities spiking in the latest year
holiday_subcomms = holiday_subcomms.\
  withColumn("score", f.when(holiday_subcomms["year"] == 2023, 2).otherwise(1))
#Only keep unique year occurences
holiday_subcomms = holiday_subcomms.dropDuplicates(["holiday", "commodity", "sub_commodity", "year"])
#Only keep 2021, 2022, 2023 data
holiday_subcomms = holiday_subcomms.filter(f.col("year") >= 2021)
#Create seasonality score based on 3 years of data
holiday_subcomms = holiday_subcomms.\
groupBy("commodity", "sub_commodity", "holiday").\
agg(f.sum("score").alias("seasonality_score"))
#At holiday/commodity level, order by score to understand most important sub-commodities
holiday_subcomms = holiday_subcomms.orderBy(["holiday", "commodity", "seasonality_score", "sub_commodity"],
                                      ascending=[True, True, False, True])
holiday_subcomms.show(50, truncate=False)

# COMMAND ----------

#Write out the file as a CSV
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

fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + "holiday_commodities.csv"
#write_out(holiday_comms, fp)
message = f"Wrote out {fp}!"
print(message)

fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + "holiday_subcommodities.csv"
#write_out(holiday_subcomms, fp)
message = f"Wrote out {fp}!"
print(message)

# COMMAND ----------

#Chunk used to write out other inputs for Excel workbook

import pandas as pd
import matplotlib.pyplot as plt
import pyspark.sql.functions as f
from statsmodels.tsa.seasonal import STL
import numpy as np
import scipy.stats as stats

#If you want to write out seasonal_commodity_history as a CSV
#fn = "seasonal_commodity_history"
#fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + fn
#df = spark.read.format("delta").load(fp)
#df = df.orderBy(["commodity", "week"])

#If you want to write out seasonal_subcommodity_history as a CSV
#fn = "seasonal_subcommodity_history"
#fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + fn
#df = spark.read.format("delta").load(fp)
#df = df.orderBy(["commodity", "sub_commodity", "week"])

#If you want to write out the holiday-week assignments as a CSV
fn = "holidays"
fp = my_dir + fn
df = spark.read.format("delta").load(fp) 
df = df.select("date", "week", "holiday")
df.show(50, truncate=False)
del(fp)

#Write out the file as a CSV
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

fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + fn
#write_out(df, fp)
message = f"Wrote out {fp}!"
print(message)
