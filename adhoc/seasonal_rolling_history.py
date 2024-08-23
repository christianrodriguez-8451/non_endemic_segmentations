# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.window import Window

my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"
fn = "raw_weekly_history"
fp = my_dir + fn
hist = spark.read.format("delta").load(fp)

window_sizes = [4, 13, 26, 52]
for window_size in window_sizes:
  #window_size = 4
  #Get rolling x week sum
  a = (window_size-1)*(-1)
  rolling_window = Window().partitionBy('sub_commodity').orderBy('week').rowsBetween(a, 0)
  rolling_hist = hist.withColumn('rolling_week_sum', f.sum('dollars_spent').over(rolling_window))
  rolling_hist = rolling_hist.filter(f.col("week") >= (window_size-1))

  #For each week, get share rank when considering rolling window
  ranking_window = Window().partitionBy('week').orderBy(f.desc('rolling_week_sum'))
  rolling_hist = rolling_hist.withColumn('rolling_share_rank', f.rank().over(ranking_window))

  #Rename columns to distinguish metrics between rolling datasets
  rolling_hist = rolling_hist.select("week", "sub_commodity", "rolling_week_sum", "rolling_share_rank")
  rolling_hist = rolling_hist.orderBy("week", "rolling_share_rank")
  rolling_hist = rolling_hist.withColumnRenamed("rolling_week_sum", str(window_size)+"w_dollars_spent")
  rolling_hist = rolling_hist.withColumnRenamed("rolling_share_rank", str(window_size)+"w_share_rank")

  #Output each dataset
  output_fp = my_dir + "rolling_{}week_history".format(window_size)
  rolling_hist.write.mode("overwrite").format("delta").save(output_fp)
  rolling_hist.show(50, truncate=False)

# COMMAND ----------

#Alcohol up to 26 weeks
#Broad brush - anybody who buys these belongs in it
