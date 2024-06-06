# Databricks notebook source
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
from effodata import ACDS, golden_rules
import resources.config as config

# Initialize a Spark session
spark = SparkSession.builder.appName("seasonal_spending").getOrCreate()


#def daily_plots():

def department_plots(df):
  """
  """
  #Get all unique departments in dataset
  departments = df.select("department").dropDuplicates().collect()
  departments = [x["department"] for x in departments]

  for dept in departments:
    temp = df.filter(f.col("department") == dept)
    
    # Aggregate data
    aggregated_df = temp.\
      groupBy("date").\
      agg(collect_list("dollars_spent").alias("dollars_spent_list"))
    # Convert to Pandas DataFrame
    pandas_df = aggregated_df.toPandas()
    # Explode the list column to separate rows
    exploded_df = pandas_df.explode('dollars_spent_list')
    # Rename columns for clarity
    exploded_df.columns = ['date', 'dollars_spent']

    # Create the box-and-whiskers plot
    plt.figure(figsize=(12, 6))
    sns.boxplot(x='date', y='dollars_spent', data=exploded_df)
    plt.title('Box-and-Whiskers Plot of Dollars Spent Across Time ({})'.format(dept))
    plt.xlabel('Date')
    plt.ylabel('Dollars Spent')
    plt.xticks(rotation=45)
    plt.show()

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

z = dept_df.select("department").dropDuplicates().collect()
z = [x["department"] for x in z]
z

# COMMAND ----------

first_date = 20230501
dept = "GROCERY"

temp = dept_df.filter(f.col("date") >= first_date)
temp = temp.filter(f.col("department") == dept)
temp = temp.filter(f.col("dollars_spent") <= 50)
temp.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Aggregate data
aggregated_df = temp.groupBy("date").agg(collect_list("dollars_spent").alias("dollars_spent_list"))

# Convert to Pandas DataFrame
pandas_df = aggregated_df.toPandas()

# Explode the list column to separate rows
exploded_df = pandas_df.explode('dollars_spent_list')

# Rename columns for clarity
exploded_df.columns = ['date', 'dollars_spent']

# Create the box-and-whiskers plot
plt.figure(figsize=(12, 6))
sns.boxplot(x='date', y='dollars_spent', data=exploded_df)
plt.title('Violin Plot of Dollars Spent per Date')
plt.xlabel('Date')
plt.ylabel('Dollars Spent')
plt.xticks(rotation=45)
plt.show()

# COMMAND ----------

department_plots(dept_df)

# COMMAND ----------


