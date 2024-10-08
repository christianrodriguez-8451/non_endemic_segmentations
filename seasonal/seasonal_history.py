# Databricks notebook source
"""
This is the code used to create the inputs for
seasonal_outlier_detection.py.

Pulls ACDS across a four year time span (12-29-2019 to 01-07-2024),
creates a timeline at the weekl level, defines the seasonal date
ranges for the past four years, and aggregate the transaction data
at the weekly level. When the data is calculated at the weekly level,
we groupby at the commodity level and then aggregate on net_spend_amt
(dollars spent) and scn_unt_qy (units bought). We also calculate the same
data at the sub-commodity level too. Four critical datasets
are outputted:

  1) seasonal_commodity_history := this is a delta file that contains
  for each commodity dollars spent/units sold at the weekly level.
  Used as an input in ***.

  2) seasonal_subcommodity_history := same thing as #1, but at the
  sub-commodity level. Used as an input in ***.

  3) timeline := this is a delta file that contains which week each
  date belongs in the timeline from 12-29-2019 to 01-07-2024.

  4) holidays := this is a delta file that contains the defined timeframe 
  for each season in each year (2020, 2021, 2022, 2023).

The is the first step to run in the seasonal audience
creation/update process. When this is updated to accomdate for the
latest year of data, make sure to adjust the start_date_str and end_date_str
variables to your desired range.
"""

# COMMAND ----------

#Idea: Re-consider how far to look at data for (not just 3 day window)
#Analyze when trips spike
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.window import Window

from effodata import ACDS, golden_rules
import resources.config as config

# Initialize a Spark session
spark = SparkSession.builder.appName("seasonal_history").getOrCreate()

# Define start and end dates
start_date_str = "20191229"
#start_date_str = "20231225"
end_date_str = "20240107"
# Convert to datetime objects for ACDS
start_date = datetime.strptime(start_date_str, '%Y%m%d')
end_date = datetime.strptime(end_date_str, '%Y%m%d')

#Give each date a week assignment
#Get range of dates
days = [(start_date + timedelta(days=i)).strftime('%Y%m%d') for i in range((end_date - start_date).days + 1)]
#Get week assignment per date
num_weeks = int(len(days)/7)
weeks = list(range(0, num_weeks + 1))
weeks = weeks * 7
weeks.sort()
#Turn the list of dates + week into a PySpark df
dates = spark.createDataFrame([(day, week) for day, week in zip(days, weeks)], ['date', 'week'])

#Get the week's enddates by group-bying at the week and getting the max value in date
weeks_enddates = dates.groupBy("week").agg(f.max("date").alias("week_enddate"))
dates = dates.join(weeks_enddates, "week", "inner")
dates = dates.withColumn("week_enddate_str", f.col("week_enddate").cast("string"))
dates = dates.withColumn(
  "week_enddate",
  f.concat(f.substring("week_enddate_str", 5, 2),
           f.lit("-"),
           f.substring("week_enddate_str", 7, 2),
           f.lit("-"), f.substring("week_enddate_str", 1, 4)
  )
)

#Assign each date their year
conditions = [
    (f.col("date").between("20190101", "20191231"), "2019"),
    (f.col("date").between("20200101", "20201231"), "2020"),
    (f.col("date").between("20210101", "20211231"), "2021"),
    (f.col("date").between("20220101", "20221231"), "2022"),
    (f.col("date").between("20230101", "20231231"), "2023"),
    (f.col("date").between("20240101", "20241231"), "2024"),
]
dates = dates.withColumn("year", f.when(conditions[0][0], conditions[0][1])
                                 .when(conditions[1][0], conditions[1][1])
                                 .when(conditions[2][0], conditions[2][1])
                                 .when(conditions[3][0], conditions[3][1])
                                 .when(conditions[4][0], conditions[4][1])
                                 .when(conditions[5][0], conditions[5][1])
)
dates = dates.select("date", "week", "week_enddate", "year")
dates = dates.orderBy("date")

my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"
fn = "timeline"
fp = my_dir + fn
dates.write.mode("overwrite").format("delta").save(fp)
dates.show(50, truncate=False)

# COMMAND ----------

#Assign each week a holiday_flag of 1 if it includes the holiday
#or any of the three days leading up to the holiday
#dict that contains the major holidays and their start dates for 
#2020, 2021, 2022, 2023
#Add veteran's day?
#Back-to-school - July 4th to september (bag snacks / school supplies). Bigger window makes sense since a lot
#of products are not perishable.
holiday_dict = {
  "new_years_eve": [20191231, 20201231, 20211231, 20221231, 20231231],
  "super_bowl": [20200202, 20210207, 20220213, 20230212],
  "valentines_day": [20200214, 20210214, 20220214, 20230214],
  "st_patricks_day": [20200317, 20210317, 20220317, 20230317],
  "easter": [20200412, 20210404, 20220417, 20230409],
  "may_5th": [20200505, 20210505, 20220505, 20230505], #Not really popular but added in for curiosity
  "mothers_day": [20200510, 20210509, 20220508, 20230514],
  "memorial_day": [20200525, 20210531, 20220530, 20230529],
  "juneteenth": [20220619, 20230619], #In 2021, not all states adopted as federally paid holiday. In 2022, most do.
  "fathers_day": [20200621, 20210620, 20220619, 20230618], #Not really popular but added in for curiosity
  "july_4th": [20200704, 20210704, 20220704, 20230704],
  "back_to_school": [20200822, 20210821, 20220813, 20230819],
  "labor_day": [20200907, 20210906, 20220905, 20230904],
  "halloween": [20201031, 20211031, 20221031, 20231031],
  "thanksgiving": [20201126, 20211125, 20221124, 20231123],
  "christmas_eve": [20201224, 20211224, 20221224, 20231224],
}
#Create a row for each holiday
rows = []
for holiday, days in holiday_dict.items():
  for day in days:
    rows.append((day, holiday, 1))

    #Add the rows for the three days leading up to the holiday
    holiday_date = datetime.strptime(str(day), "%Y%m%d")
    for i in range(1, 4):
      days_before = holiday_date - timedelta(days=i)
      rows.append((int(days_before.strftime("%Y%m%d")), holiday, 1))

#Define schema for holiday dataframe
schema = t.StructType([
    t.StructField("date", t.IntegerType(), False),
    t.StructField("holiday", t.StringType(), False),
    t.StructField("holiday_flag", t.IntegerType(), False),
])
#Use rows and schema to create holiday dataframe with holiday_flag
holidays = spark.createDataFrame(rows, schema=schema)
#Merge dates to get what specific weeks the holidays covered
holidays = holidays.join(dates.select("date", "week"),
                         "date",
                         "inner")
holidays = holidays.orderBy("holiday", "date")

my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"
fn = "holidays"
fp = my_dir + fn
holidays.write.mode("overwrite").format("delta").save(fp)
holidays.show(50, truncate=False)

# COMMAND ----------

#Pull in transaction data and keep only ehhn, gtin_no, and dollars_spent
acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))
acds = acds.select("gtin_no", "net_spend_amt", "scn_unt_qy", f.col("trn_dt").alias("date"))
#QUESTION: Should I be dropping returns or any other weird cases?
acds.cache()
acds.show(truncate=False)

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

#QUESTION: Do I need to pull all PIM cycles to correctly assign commodities/sub-commodities?
#Does Kroger go back and assign correct UPCs as they change?

# COMMAND ----------

#Sum net_spend_amt by sub-commodity and date to ease computation down the road
acds = acds.join(pim, "gtin_no", "inner")
acds = acds.\
  groupby("commodity", "sub_commodity", "date").\
  agg(f.sum("net_spend_amt").alias("net_spend_amt"),
      f.sum("scn_unt_qy").alias("scn_unt_qy"))
acds = acds.join(dates, "date", "inner")
acds.cache()
acds.show(50, truncate=False)

# COMMAND ----------

#Create commodity history by calculating dollars spent and dollar share per week.
comm_hist = acds.\
  groupby("commodity", "week").\
  agg(f.sum("net_spend_amt").alias("dollars_spent"),
      f.sum("scn_unt_qy").alias("units_sold"))
comm_hist = comm_hist.orderBy(['commodity', 'week'])

fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + "seasonal_commodity_history"
comm_hist.write.mode("overwrite").format("delta").save(fp)
comm_hist.show(50, truncate=False)

# COMMAND ----------

#Create sub-commodity history by calculating dollars spent and dollar share per week.
subcomm_hist = acds.\
  groupby("commodity", "sub_commodity", "week").\
  agg(f.sum("net_spend_amt").alias("dollars_spent"),
      f.sum("scn_unt_qy").alias("units_sold"))
subcomm_hist = subcomm_hist.orderBy(['commodity', 'sub_commodity', 'week'])

fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + "seasonal_subcommodity_history"
subcomm_hist.write.mode("overwrite").format("delta").save(fp)
subcomm_hist.show(50, truncate=False)
