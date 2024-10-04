# Databricks notebook source
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window
from effodata import ACDS, golden_rules
import resources.config as config

#Holiday dataframe that contains the date ranges of our holidays
my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"
fn = "holidays"
fp = my_dir + fn
holidays = spark.read.format("delta").load(fp)

#These dates are used to filter down history only to relevant dates
keep_dates = holidays.select("date").dropDuplicates()
keep_dates.show(50, truncate=False)

# COMMAND ----------

# Define start and end date for ACDS pull
start_date_str = "20191229"
end_date_str = "20240107"
# Convert to datetime objects for ACDS
start_date = datetime.strptime(start_date_str, '%Y%m%d')
end_date = datetime.strptime(end_date_str, '%Y%m%d')

acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))
acds = acds.select("ehhn", "gtin_no", "net_spend_amt", f.col("trn_dt").alias("date"))
#Only keep the holiday days to keep the computation short
acds = acds.join(keep_dates, "date", "inner")
acds.show(50, truncate=False)

# COMMAND ----------

#Pull upc hierarchy from PIM to assign sub-commodity to transactions
pim_fp = config.get_latest_modified_directory(config.azure_pim_core_by_cycle)
pim = config.spark.read.parquet(pim_fp)
pim = pim.select(
  f.col("upc").alias("gtin_no"),
  f.col("familyTree.commodity").alias("commodity"),
  f.col("familyTree.subCommodity").alias("sub_commodity"),
)
pim = pim.withColumn("commodity", f.col("commodity").getItem("name"))
pim = pim.withColumn("sub_commodity", f.col("sub_commodity").getItem("name"))
pim = pim.na.drop(subset=["sub_commodity"])

acds = acds.join(pim, "gtin_no", "inner")
acds.cache()
acds.show(50, truncate=False)

# COMMAND ----------

#Read in the dataframe that contains the popular sub-commodities for each holiday.
my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"
fn = "holiday_sub_commodities_95.csv"
holidays_subcoms = spark.read.csv(my_dir + fn, header=True, inferSchema=True)
holidays_subcoms.show(50, truncate=False)

# COMMAND ----------

holidays_list = holidays.select("holiday").rdd.flatMap(lambda x: x).collect()
holidays_list = list(set(holidays_list))

my_holiday = holidays_list[0]
#for my_holiday in holidays_list:
#Pull only the relevant sub-commodities for the given holiday.
subcoms = holidays_subcoms.filter(f.col("holiday") == my_holiday)
subcoms = subcoms.select("sub_commodity").rdd.flatMap(lambda x: x).collect()
subcoms = list(set(subcoms))

#Keep only the dates relevent to the given holiday.
keep_dates = holidays.filter(f.col("holiday") == my_holiday)
keep_dates = keep_dates.select("date")
temp = acds.join(keep_dates, "date", "inner")

#Calculate how much each household is spending on the given sub-commodities set.
temp = temp.filter(f.col("sub_commodity").isin(subcoms))
temp = temp.groupBy("ehhn").agg(f.sum("net_spend_amt").alias("dollars_spent"))
percentiles = temp.approxQuantile("dollars_spent", [0.33, 0.66], 0.00001)
temp = temp.withColumn(
  "segment",
  f.when(f.col("dollars_spent") < percentiles[0], "L")
      .otherwise(f.when(f.col("dollars_spent") > percentiles[1], "H")
                .otherwise("M"))
)
temp.show(50, truncate=False)

#QC the methodology
counts = temp.groupBy("segment").count()
avgs = temp.groupBy("segment").agg(f.avg(f.col("dollars_spent")).alias("dollars_spent"))
print(f"{my_holiday} Counts:\n")
counts.show()
print(f"{my_holiday} Averages:\n")
avgs.show()
