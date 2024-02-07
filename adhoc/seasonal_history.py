# Databricks notebook source
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
holidays.show(50, truncate=False)

# COMMAND ----------

#Pull in transaction data and keep only ehhn, gtin_no, and dollars_spent
acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))
acds = acds.select("gtin_no", "net_spend_amt", f.col("trn_dt").alias("date"))
#QUESTION: Should I be dropping returns or any other weird cases?
acds.cache()
acds.show(truncate=False)
#Get units sold

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
acds = acds.groupby("sub_commodity", "date").agg(f.sum("net_spend_amt").alias("net_spend_amt"))
acds = acds.join(dates, "date", "inner")
acds.cache()
acds.show(50, truncate=False)

# COMMAND ----------

#This chunk is very important! It creates weekly history for all
#sub-commodities for 2020, 2021, 2022, and 2023.
#Each week contains: dollars spent on sub-commodities,
#total dollars spent on all sub-commodities, and
#rank of dollar share. In a way, each sub-commodity has their
#own time series. May be able to fit an outlier detection system
#on sub-commodity history.

#Create sub-commodity history by calculating dollars spent and dollar share per week.
subcomm_hist = acds.groupby("sub_commodity", "week").agg(f.sum("net_spend_amt").alias("dollars_spent"))
total_hist = subcomm_hist.groupby("week").agg(f.sum("dollars_spent").alias("total_dollars_spent"))
subcomm_hist = subcomm_hist.join(total_hist, "week", "inner")
subcomm_hist = subcomm_hist.orderBy(['sub_commodity', 'week'])
subcomm_hist = subcomm_hist.withColumn('dollar_share', subcomm_hist['dollars_spent'] / subcomm_hist['total_dollars_spent'])

#Rank each subcommodity an assortment rank within each week.
#The rank is based on dollar share at the given week.
window_spec = Window().partitionBy('week').orderBy(f.desc('dollar_share'))
subcomm_hist = subcomm_hist.withColumn('share_rank', f.rank().over(window_spec))
subcomm_hist.cache()

subcomm_hist = subcomm_hist.orderBy(['week', 'share_rank'])
subcomm_hist.show(25, truncate=False)

# COMMAND ----------

my_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/"

fn = "timeline"
fp = my_dir + fn
dates.write.mode("overwrite").format("delta").save(fp)

fn = "holidays"
fp = my_dir + fn
holidays.write.mode("overwrite").format("delta").save(fp)

fn = "raw_weekly_history"
fp = my_dir + fn
subcomm_hist.write.mode("overwrite").format("delta").save(fp)

# COMMAND ----------

#Write out: dates, holidays, subcomm_hist

#Next:
#Take data and calculate dollars_spent, total_dollars_spent, dollar_share, and share_rank in x week rolling basis.
#Do for: 4 week rolling, 13 week rolling, 26 week rolling, 52 week rolling
#Write each out as separate datasets. Merge them together and write-out in another script.

#In another script, take final history data and do clustering on the isolated
#holiday occurences. In each occurence, take the cluster with the average
#higher shift in share ranks.

#Want holiday occurences for: 2021, 2022, 2023

# COMMAND ----------

#Start of Emily Singer's methodology

#Create sub-commodity history by calculating dollars spent and dollar share per year.
subcomm_annual_hist = acds.groupby("sub_commodity", "year").agg(f.sum("net_spend_amt").alias("dollars_spent"))
total_annual_hist = subcomm_annual_hist.groupby("year").agg(f.sum("dollars_spent").alias("total_dollars_spent"))
subcomm_annual_hist = subcomm_annual_hist.join(total_annual_hist, "year", "inner")
subcomm_annual_hist = subcomm_annual_hist.orderBy(['sub_commodity', 'year'])
subcomm_annual_hist = subcomm_annual_hist.withColumn('dollar_share', subcomm_annual_hist['dollars_spent'] / subcomm_annual_hist['total_dollars_spent'])

#Rank each subcommodity an assortment rank within each year.
#The rank is based on dollar share at the given year.
window_spec = Window().partitionBy('year').orderBy(f.desc('dollar_share'))
subcomm_annual_hist = subcomm_annual_hist.withColumn('share_rank', f.rank().over(window_spec))
subcomm_annual_hist.cache()

subcomm_annual_hist = subcomm_annual_hist.orderBy(['year', 'share_rank'])
subcomm_annual_hist.show(25, truncate=False)

# COMMAND ----------

subcomm_annual_hist.filter(f.col("year") == 2020).show(50, truncate=False)

# COMMAND ----------

subcomm_annual_hist.filter(f.col("year") == 2021).show(50, truncate=False)

# COMMAND ----------

subcomm_annual_hist.filter(f.col("year") == 2022).show(50, truncate=False)

# COMMAND ----------

subcomm_annual_hist.filter(f.col("year") == 2023).show(50, truncate=False)

# COMMAND ----------

#Get top 500 sub-commodities at weekly and annual level
top_500_w = subcomm_hist.filter(f.col("share_rank") <= 500)
top_500_a = subcomm_annual_hist.filter(f.col("share_rank") <= 500)

#Assign year to weekly ranks to compare to appropiate annual ranks
conditions = [
    (f.col("week").between("0", "51"), "2020"),
    (f.col("week").between("52", "104"), "2021"),
    (f.col("week").between("105", "156"), "2022"),
    (f.col("week").between("157", "209"), "2023"),
    (f.col("week").between("210", "262"), "2024"),
]
top_500_w = top_500_w.withColumn("year", f.when(conditions[0][0], conditions[0][1])
                                          .when(conditions[1][0], conditions[1][1])
                                          .when(conditions[2][0], conditions[2][1])
                                          .when(conditions[3][0], conditions[3][1])
                                          .when(conditions[4][0], conditions[4][1]))
top_500_a = top_500_a.withColumnRenamed("share_rank", "annual_share_rank",)
top_500_w = top_500_w.join(top_500_a.select("year", "sub_commodity", "annual_share_rank"),
                           ["year", "sub_commodity"],
                           "left")
top_500_w = top_500_w.withColumn("share_rank_diff", f.col("annual_share_rank") - f.col("share_rank"))
#Assign holidays to the weeks
top_500_w = top_500_w.join(holidays.select("week", "holiday_flag", "holiday").dropDuplicates(),
                            "week",
                            "left")
top_500_w = top_500_w.fillna(0, subset="holiday_flag")
#Pull any sub-commodity that is new to top 500 (week vs annual) or moves up more than 10 ranks
conditions = (
  ((f.col("share_rank_diff") >= 10) | (f.col("annual_share_rank").isNull()))
  & (f.col("holiday_flag") == 1)
)
holiday_subcomms = top_500_w.filter(conditions)
holiday_subcomms.cache()

holiday_subcomms.show(50, truncate=False)

# COMMAND ----------

temp = holiday_subcomms.select("week", "year", "holiday", "sub_commodity", "share_rank", "annual_share_rank", "share_rank_diff")
#temp = temp.filter(f.col("holiday") == "july_4th")
temp = temp.orderBy(["holiday", "share_rank_diff"])
temp.show(50, truncate=False)

# COMMAND ----------

import os

temp_target = os.path.dirname(fps[0])
temp_target

# COMMAND ----------

import os

#def write_out(df, fp, delimiter="^", format="csv"):
#Placeholder filepath for intermediate processing
temp_target = os.path.dirname(fp)
real_target = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + "holiday_subcommodities"

#Write out df as partitioned file. Write out ^ delimited
output_df = temp
output_df.coalesce(1).write.options(header=True, delimiter="^").mode("overwrite").format('csv').save(temp_target)

#Copy desired file from parititioned directory to desired location
temporary_fp = os.path.join(temp_target, dbutils.fs.ls(temp_target)[3][1])
print(temporary_fp)
dbutils.fs.cp(temporary_fp, real_target)
dbutils.fs.rm(temp_target, recurse=True)

# COMMAND ----------

subcomm_hist = subcomm_hist.join(dates.select("week", "week_enddate").dropDuplicates(),
                                 "week",
                                 "inner")
subcomm_hist = subcomm_hist.join(holidays.select("week", "holiday_flag").dropDuplicates(),
                                 "week",
                                 "left")
subcomm_hist = subcomm_hist.fillna(0, subset="holiday_flag")

#Round to 2 sig figs for easy readability and make import to Excel easy
subcomm_hist = subcomm_hist.withColumn(
    "dollar_share",
    f.round(f.col("dollar_share") * 100, 2)
)
subcomm_hist = subcomm_hist.withColumn(
    "dollars_spent",
    f.round(f.col("dollars_spent"), 2)
)
subcomm_hist = subcomm_hist.withColumn(
    "total_dollars_spent",
    f.round(f.col("dollars_spent"), 2)
)

#Change column order for readability
subcomm_hist = subcomm_hist.select(
  "year", "week", "holiday_flag", "week_enddate",
  "sub_commodity", "share_rank",
  "dollars_spent", "total_dollars_spent", "dollar_share",
)
#One way to order the data: top x sub-commodities per week
subcomm_hist = subcomm_hist.orderBy(['week', 'share_rank'])
subcomm_hist.show(50, truncate=False)
