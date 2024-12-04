# Databricks notebook source
"""
Creates segmentations by conducting the following:

  1) Pull the latest 52 weeks of KPI data of all enterprise
  activity for every household. Units are aggregated at
  the household-quarter level. This is necessary
  for unit penetration calculation.

  2) Pull the latest 52 weeks of KPI data for every household
  specifically for the given audience's UPC list. Sales and
  units are aggregated at the household-quarter level.

  3) Do an inner merge between the data from step 1 and
  step 2. For each household and quarter, calculate
  unit penetration by dividing quarterly units by
  total units.

  4) Filter out households that are considered inactive.
  A household is considered active if it satisfies both
  of the following conditions:
    
    a) The household shopped at Kroger at least 7 times
    in the past year.
    
    b) The household shopped at least once in each of
    the 3 latest quarters.

  5) For each quarter, assign each household a spend
  rank and a penetration rank. If the household is in
  the top 90th percentile of spending for the given quarter,
  then the household is given a spend rank of H for that quarter.
  If the household is in the top 50th percentile but not
  in the top 90th percentile, then the household is given 
  a spend rank of M for that quarter. Otherwise, the household
  is given a spend rank of L for that quarter. Penetration rank
  is assigned identically, but unit penetration is used
  instead.

  6) For each household and quarter, assign them points
  based on their spend rank and penetration rank. The
  point criteria is below:

    Spend Rank H + Penetration Rank H = 3 points
    Spend Rank H + Penetration Rank M = 3 points
    Spend Rank M + Penetration Rank H = 3 points

    Spend Rank M + Penetration Rank M = 2 points
    Spend Rank L + Penetration Rank H = 2 points
    Spend Rank H + Penetration Rank L = 2 points

    Spend Rank L + Penetration Rank L = 1 point
    Spend Rank L + Penetration Rank M = 1 point
    Spend Rank M + Penetration Rank L = 1 point

  7) For each household, a final score by taking a
  weighted average of all their quarter points. The formula
  of the weighted average is below:

    Final Score = 0.23*(Q1 point) + 0.24*(Q2 point) + 0.26*(Q3 point) + 0.27*(Q4 point)

  The weights are specified that way to promote recency.

  8) For each household, assign them their final propensity
  rank. If a household has a final score greater than 2, then
  assign them a propensity score of H. If a household has a final
  score greater than 1 but less than or equal to 2, then assign
  them a propensity score of M. Otherwise, assign them a propensity
  score of L.

  9) Output the audience's households and their propensity
  scores.

This code is a streamlined and simplified version of ***
and ***. The prior code had unnecessary complexities that
made the process a black box and caused bugs in production.
Some complexities we omitted from the current version are:
  a) Vintage files

  b) Fiscal week

  c) Stratum week

  d) New, lapsed, and inconsistent propensity rank designation

To Do:
  1) Break up this code into 3 smaller modules:

    i) Pulls and writes out the KPI data. Basically does step 1
    to step 4 of the described process.

    ii) Calculates statistics and conducts quality checks for the data
    that was outputted from module i.

    iii) Takes the data from module i and assigns propensity scores.

  2) Consider adding back in new, lapsed, and inconsistent capabilities?
"""

# COMMAND ----------

spark.conf.set('spark.sql.shuffle.partitions', 'auto')

# COMMAND ----------

#Python packages
import math as m
import pandas as pd
import datetime as dt
import dateutil.relativedelta as dr
import pyspark.sql.functions as f
from pyspark.sql.window import Window

#Local module
import toolbox.config as con

#84.51 packages
from effodata import ACDS, golden_rules, Joiner, Sifter, Equality, sifter, join_on, joiner
from kpi_metrics import (
    KPI,
    AliasMetric,
    CustomMetric,
    AliasGroupby,
    Rollup,
    Cube,
    available_metrics,
    get_metrics
)
kpi = KPI(use_sample_mart=False)

# COMMAND ----------

#Build the timeline - past 52 weeks
#First 13 weeks belong to quarter 1
#Last 13 weeks belong to quarter 4
days = list(range(1, (52*7)+1))
weeks = list(range(1, 52+1)) * 7
quarters = list(range(1, 4+1)) * (7*13)
#Assign weights to each quarter - more weight given to more
#recent quarters. This is to promote recency.
weights = [0.23]*13*7 + [0.24]*13*7 + [0.26]*13*7 + [0.27]*13*7
assert len(days) == len(weeks)
assert len(weeks) == len(quarters)
assert len(quarters) == len(weights)
days.sort()
weeks.sort()
quarters.sort()
data = {
  'day': days,
  'week': weeks,
  'quarter': quarters,
  'weight': weights,
}
quarters = pd.DataFrame(data)
quarters = spark.createDataFrame(quarters)

quarters.cache()
quarters.display()

# COMMAND ----------

#Define the start and end date of the data pulls
today = dt.date.today()
last_monday = today - dr.datetime.timedelta(days=today.weekday())
start_date = last_monday - dr.datetime.timedelta(weeks=52)
end_date = last_monday - dr.datetime.timedelta(days=1)

#start_date / end_date format - YYYYMMDD (str)
#YYYYMMDD format is required for ACDS/KPI usage
start_date = start_date.strftime('%Y%m%d')
#We use unformatted_end_date for a function that calculates
#day in extract_time. It is used in pyspark.sql.functions.datediff
unformatted_end_date = end_date
end_date = end_date.strftime('%Y%m%d')

message = "Start Date: {}\nEnd Date: {}".format(start_date, end_date)
print(message)
del(message, last_monday)

# COMMAND ----------

def extract_time(df, end_date):
  """

  df :=
  end_date := 
  """

  #Add in a day and week column in your acds pull to enable easy querying
  df = df.\
    withColumn('year', f.substring('trn_dt', 1, 4)).\
    withColumn('month', f.substring('trn_dt', 5, 2)).\
    withColumn('day', f.substring('trn_dt', 7, 2))
  df = df.withColumn('date', f.concat(f.col('month'), f.lit("-"), f.col("day"), f.lit("-"), f.col("year")))
  df = df.withColumn("transaction_date", f.to_date(f.col("date"), "MM-dd-yyyy"))
  df = df.withColumn("end_date", f.lit(end_date))
  df = df.drop("day")
  df = df.withColumn("day", f.datediff(f.col("transaction_date"), f.col("end_date")))
  df = df.withColumn("day", f.col("day")+364)
  df = df.withColumn("day", f.round("day"))
  df = df.withColumn("week", f.col("day")/7)
  df = df.withColumn("week", f.ceil(f.col("week")))
  for i in ["month", "year", "date", "transaction_date", "end_date"]:
    df = df.drop(i)
  
  return(df)

# COMMAND ----------

#Pull in enterprise level KPI data
ent_kpis = kpi.get_aggregate(
    start_date=start_date,
    end_date=end_date,
    metrics=["sales", "gr_visits", "units"],
    join_with='stores',
    apply_golden_rules=golden_rules(),
    group_by=["ehhn", "trn_dt", "geo_div_no"],
).where(f.col('ehhn').isNotNull())
ent_kpis = extract_time(ent_kpis, unformatted_end_date)
ent_kpis = ent_kpis.join(quarters, ["day", "week"])

#Aggregate total units sold at quarterly level. Will use this to
#calculate unit_penetration for the given UPC list.
ent_units = ent_kpis.\
  groupBy('ehhn', 'quarter').\
  agg(f.sum('units').alias('enterprise_units'))
ent_units.cache()

# COMMAND ----------

audience_name = "vegetarian"
#Pull in the latest upc file for the given audience
audience = con.segmentation(audience_name)
upc_fp = audience.upc_directory + audience.upc_files[-1]
upc_df = spark.read.format("delta").load(upc_fp)

#Pull UPC-list level KPI data
ehhn_upc_kpis = kpi.get_aggregate(
  start_date=start_date,
  end_date=end_date,
  metrics=["sales", "gr_visits", "units"],
  join_with='stores',
  apply_golden_rules=golden_rules(),
  group_by=["ehhn", "trn_dt", "geo_div_no"],
  filter_by=Sifter(upc_df, join_cond=Equality("gtin_no"), method="include")
).where(f.col('ehhn').isNotNull())
ehhn_upc_kpis = extract_time(ehhn_upc_kpis, unformatted_end_date)
ehhn_upc_kpis = ehhn_upc_kpis.join(quarters, ["day", "week"])

#Aggregate sales + units to quarterly level. Households are available
#based on behavior across the quarters.
ehhn_upc_kpis = ehhn_upc_kpis.\
  groupBy(['ehhn', 'quarter']).\
  agg(f.sum('sales').alias('quarterly_sales'),
      f.sum('units').alias('quarterly_units'),
      f.sum('gr_visits').alias('quarterly_visits'))
ehhn_upc_kpis.cache()

# COMMAND ----------

#(Units sold in UPC list)/(Total Units Sold Altogether) =  unit_penetration
#We get unit_penetration at the quarterly level
ehhn_upc_kpis = ehhn_upc_kpis.\
  join(ent_units, ['ehhn', 'quarter'], 'inner').\
  withColumn('unit_penetration',
             f.round(f.col('quarterly_units') / f.col('enterprise_units'), 4))
ehhn_upc_kpis.cache()

# COMMAND ----------

ehhn_upc_kpis.limit(1500).display()

# COMMAND ----------

#Before we evaluate each ehhn as HML given the audience's UPC list,
#we need to limit the evaluation to only ehhns buying from the UPC list
#somewhat consistently. The ehhns rarely buying from the UPC list
#adds noise. Think about it - if an ehhn shops only once at Kroger, but
#bought strictly from the UPC list then they will get a unit penetration
#of 1. This gives them a penetration rank of 1 which in turn gives them
#at least an overall rank of M for that given quarter.

#Active shopper criteria 1: has shopped at least 7 times in the past year
mininum_visits = ehhn_upc_kpis.\
  filter(f.col("quarterly_sales") > 0).\
  groupBy("ehhn").\
  agg(f.sum("quarterly_visits").alias("total_visits")).\
  filter(f.col("total_visits") >= 7).\
  select("ehhn")

#Active shopper criteria 2: shopped in the last 3 most recent quarters
minimum_quarters = ehhn_upc_kpis.\
  filter(f.col("quarterly_sales") > 0).\
  filter(f.col("quarter") > 1).\
  groupBy("ehhn").\
  agg(f.countDistinct("quarter").alias("quarter_count")).\
  filter(f.col("quarter_count") == 3).\
  select("ehhn")

active_ehhns = mininum_visits.join(minimum_quarters, "ehhn")
ehhn_upc_kpis = ehhn_upc_kpis.join(active_ehhns, "ehhn")
del(mininum_visits, minimum_quarters, active_ehhns)

# COMMAND ----------

#HML rank is actually determined based on a combination of
#spending HML rank and penetration HML rank.

#Define thresholds for spend HML
high_spend_cutoff = 0.9
medium_spend_cutoff = 0.5

#Define thresholds for penetration HML
high_visits_cutoff = 0.9
medium_visits_cutoff = 0.5

#For each quarter, assign spending rank to each ehhn depending on their spending.
spend_window = Window.partitionBy('quarter').orderBy('quarterly_sales')
ehhn_upc_kpis = ehhn_upc_kpis.withColumn(f'spend_percentile', f.percent_rank().over(spend_window))
ehhn_upc_kpis = ehhn_upc_kpis.withColumn(
  'spend_rank',
  f.when(f.col('spend_percentile') >= high_spend_cutoff, 'H').\
    when(f.col('spend_percentile') >= medium_spend_cutoff, 'M').\
    otherwise('L')
)

#For each quarter, assign penetration rank to each ehhn depending on their penetration.
penetration_window = Window.partitionBy('quarter').orderBy('unit_penetration')
#Special case: if unit penetration is 1, then it is H for all quarters
#This implies everything they buy is in the given UPC list!
ehhn_upc_kpis = ehhn_upc_kpis.withColumn(
  'penetration_percentile',
  f.when(f.col(f'unit_penetration') == 1, 1).\
  otherwise(f.percent_rank().over(penetration_window))
)
ehhn_upc_kpis = ehhn_upc_kpis.withColumn(
  'penetration_rank',
  f.when(f.col('penetration_percentile') >= high_visits_cutoff, 'H').\
    when(f.col('penetration_percentile') >= medium_visits_cutoff, 'M').\
    otherwise('L')
)

#For each ehhn, assign them points for each quarter
ehhn_upc_kpis = ehhn_upc_kpis.withColumn(
  'quarter_point',
  #If H+H, H+M, or M+H then assign 3 points for that quarter.
  f.when((f.col('spend_rank') == 'H') & (f.col('penetration_rank') == 'H'), 3).\
  when((f.col('spend_rank') == 'M') & (f.col('penetration_rank') == 'H'), 3).\
  when((f.col('spend_rank') == 'H') & (f.col('penetration_rank') == 'M'), 3).\
  #If M+M, L+H, or H+L then assign 2 points for that quarter.
  when((f.col('spend_rank') == 'M') & (f.col('penetration_rank') == 'M'), 2).\
  when((f.col('spend_rank') == 'L') & (f.col('penetration_rank') == 'H'), 2).\
  when((f.col('spend_rank') == 'H') & (f.col('penetration_rank') == 'L'), 2).\
  #If L+L, L+M, or M+L then assign 1 point for that quarter.
  when((f.col('spend_rank') == 'L') & (f.col('penetration_rank') == 'L'), 1).\
  when((f.col('spend_rank') == 'M') & (f.col('penetration_rank') == 'L'), 1).\
  when((f.col('spend_rank') == 'L') & (f.col('penetration_rank') == 'M'), 1).\
  #Otherwise, they get nothing 0 points.
  otherwise(0)
)

# COMMAND ----------

#Give each quarter_point their weight - we want to promote recency!
ehhn_upc_kpis = ehhn_upc_kpis.\
  join(quarters.select("quarter", "weight").dropDuplicates(), 'quarter').\
  withColumn(
    'recency_adjusted_quarter_point',
    f.round(f.col('quarter_point') * f.col('weight'), 4)
)

#Assign them a final and overall HML rank as a weighted average
#of their HML rank across all quarters.
ehhn_hml = ehhn_upc_kpis.\
  groupBy('ehhn').\
  agg(f.sum('recency_adjusted_quarter_point').alias('weighted_avg')).\
  withColumn('propensity',
             f.when(f.col('weighted_avg') > 2, 'H').\
               when(f.col('weighted_avg') > 1, 'M').\
               when(f.col('weighted_avg') <= 1, 'L').\
               otherwise('ERROR')
  )
ehhn_hml = ehhn_hml.select('ehhn', 'propensity')

# COMMAND ----------

#Write out ehhn_upc_kpis and ehhn_hml datasets
ehhn_hml = ehhn_hml.withColumnRenamed('propensity', 'segment')
output_fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/funlo_lite/{}/{}_{}".format(audience_name, audience_name, today.strftime('%Y%m%d'))
ehhn_hml.write.mode("overwrite").format("delta").save(output_fp)
print(f"Successfully wrote out {output_fp}!")
del(output_fp)

# COMMAND ----------

#QC1: Check counts of HML ranks
hml_counts = ehhn_hml.\
  groupBy('segment').\
  agg(f.countDistinct("ehhn").alias("ehhn_count"))
print(hml_counts.show(50, truncate=False))
del(hml_counts)

#QC2: Check counts propensities used in production
propensities = audience.propensities
count = ehhn_hml.\
  filter(f.col('segment').isin(propensities)).\
  count()
message = "{} ({}) ehhn counts: {}".format(audience_name, "+".join(propensities), count)
print(message)
del(message)

# COMMAND ----------

#How many of the ranked households DID NOT have four quarters of data?

#How does the distribution of quarterly sales for the upc list change across the quarters?

##How does the distribution of quarterly units for the upc list change across the quarters?

#How does the distribution of unit penetration change across the quarters?

# COMMAND ----------

#Figure out how new, lapsed, and inconsistent are assigned.

#where does new_market_div come from?

#Axe the following - ehhn vintages, fiscal_week, and stratum_week

# COMMAND ----------

audiences = [
  #'ayervedic',
  #'beveragist',
  #'breakfast_buyers',
  #'engine_2',
  #'free_from_gluten',
  #'glycemic',
  #'grain_free',
  #'healthy_eating',
  #'heart_friendly',
  #'high_protein',
  #'hispanic_cuisine',
  #'juicing_beveragust',
  #'ketogenic',
  #'kidney-friendly',
  #'lactose_free',
  #'low_bacteria',
  #'low_calorie',
  #'low_fodmap',
  #'low_protein',
  #'low_salt',
  #'macrobiotic',
  #'mediterranean_diet',
  #'non_veg',
  #'organic',
  #'ovo-vegetarians',
  #'paleo',
  #'pescetarian',
  #'plant_based',
  #'plant_based_whole_foods',
  #'raw_food',
  #'salty_snackers',
  #'vegan',
  #'vegetarian',
  'without_beef',
  'without_pork',
]

for audience_name in audiences:
  #Pull in the latest upc file for the given audience
  audience = con.segmentation(audience_name)
  upc_fp = audience.upc_directory + audience.upc_files[-1]
  upc_df = spark.read.format("delta").load(upc_fp)

  #Pull UPC-list level KPI data for given audience
  ehhn_upc_kpis = kpi.get_aggregate(
    start_date=start_date,
    end_date=end_date,
    metrics=["sales", "gr_visits", "units"],
    join_with='stores',
    apply_golden_rules=golden_rules(),
    group_by=["ehhn", "trn_dt", "geo_div_no"],
    filter_by=Sifter(upc_df, join_cond=Equality("gtin_no"), method="include")
  ).where(f.col('ehhn').isNotNull())
  ehhn_upc_kpis = extract_time(ehhn_upc_kpis, unformatted_end_date)
  ehhn_upc_kpis = ehhn_upc_kpis.join(quarters, ["day", "week"])

  #Aggregate sales + units to quarterly level. Households are available
  #based on behavior across the quarters.
  ehhn_upc_kpis = ehhn_upc_kpis.\
    groupBy(['ehhn', 'quarter']).\
    agg(f.sum('sales').alias('quarterly_sales'),
        f.sum('units').alias('quarterly_units'),
        f.sum('gr_visits').alias('quarterly_visits'))

  #(Units sold in UPC list)/(Total Units Sold Altogether) =  unit_penetration
  #We get unit_penetration at the quarterly level
  ehhn_upc_kpis = ehhn_upc_kpis.\
    join(ent_units, ['ehhn', 'quarter'], 'inner').\
    withColumn('unit_penetration',
              f.round(f.col('quarterly_units') / f.col('enterprise_units'), 4))

  #Before we evaluate each ehhn as HML given the audience's UPC list,
  #we need to limit the evaluation to only ehhns buying from the UPC list
  #somewhat consistently. The ehhns rarely buying from the UPC list
  #adds noise. Think about it - if an ehhn shops only once at Kroger, but
  #bought strictly from the UPC list then they will get a unit penetration
  #of 1. This gives them a penetration rank of 1 which in turn gives them
  #at least an overall rank of M for that given quarter.

  #Active shopper criteria 1: has shopped at least 7 times in the past year
  mininum_visits = ehhn_upc_kpis.\
    filter(f.col("quarterly_sales") > 0).\
    groupBy("ehhn").\
    agg(f.sum("quarterly_visits").alias("total_visits")).\
    filter(f.col("total_visits") >= 7).\
    select("ehhn")

  #Active shopper criteria 2: shopped in the last 3 most recent quarters
  minimum_quarters = ehhn_upc_kpis.\
    filter(f.col("quarterly_sales") > 0).\
    filter(f.col("quarter") > 1).\
    groupBy("ehhn").\
    agg(f.countDistinct("quarter").alias("quarter_count")).\
    filter(f.col("quarter_count") == 3).\
    select("ehhn")

  active_ehhns = mininum_visits.join(minimum_quarters, "ehhn")
  ehhn_upc_kpis = ehhn_upc_kpis.join(active_ehhns, "ehhn")
  del(mininum_visits, minimum_quarters, active_ehhns)

  #HML rank is actually determined based on a combination of
  #spending HML rank and penetration HML rank.

  #Define thresholds for spend HML
  high_spend_cutoff = 0.9
  medium_spend_cutoff = 0.5

  #Define thresholds for penetration HML
  high_visits_cutoff = 0.9
  medium_visits_cutoff = 0.5

  #For each quarter, assign spending rank to each ehhn depending on their spending.
  spend_window = Window.partitionBy('quarter').orderBy('quarterly_sales')
  ehhn_upc_kpis = ehhn_upc_kpis.withColumn(f'spend_percentile', f.percent_rank().over(spend_window))
  ehhn_upc_kpis = ehhn_upc_kpis.withColumn(
    'spend_rank',
    f.when(f.col('spend_percentile') >= high_spend_cutoff, 'H').\
      when(f.col('spend_percentile') >= medium_spend_cutoff, 'M').\
      otherwise('L')
  )

  #For each quarter, assign penetration rank to each ehhn depending on their penetration.
  penetration_window = Window.partitionBy('quarter').orderBy('unit_penetration')
  #Special case: if unit penetration is 1, then it is H for all quarters
  #This implies everything they buy is in the given UPC list!
  ehhn_upc_kpis = ehhn_upc_kpis.withColumn(
    'penetration_percentile',
    f.when(f.col(f'unit_penetration') == 1, 1).\
    otherwise(f.percent_rank().over(penetration_window))
  )
  ehhn_upc_kpis = ehhn_upc_kpis.withColumn(
    'penetration_rank',
    f.when(f.col('penetration_percentile') >= high_visits_cutoff, 'H').\
      when(f.col('penetration_percentile') >= medium_visits_cutoff, 'M').\
      otherwise('L')
  )

  #For each ehhn, assign them points for each quarter
  ehhn_upc_kpis = ehhn_upc_kpis.withColumn(
    'quarter_point',
    #If H+H, H+M, or M+H then assign 3 points for that quarter.
    f.when((f.col('spend_rank') == 'H') & (f.col('penetration_rank') == 'H'), 3).\
    when((f.col('spend_rank') == 'M') & (f.col('penetration_rank') == 'H'), 3).\
    when((f.col('spend_rank') == 'H') & (f.col('penetration_rank') == 'M'), 3).\
    #If M+M, L+H, or H+L then assign 2 points for that quarter.
    when((f.col('spend_rank') == 'M') & (f.col('penetration_rank') == 'M'), 2).\
    when((f.col('spend_rank') == 'L') & (f.col('penetration_rank') == 'H'), 2).\
    when((f.col('spend_rank') == 'H') & (f.col('penetration_rank') == 'L'), 2).\
    #If L+L, L+M, or M+L then assign 1 point for that quarter.
    when((f.col('spend_rank') == 'L') & (f.col('penetration_rank') == 'L'), 1).\
    when((f.col('spend_rank') == 'M') & (f.col('penetration_rank') == 'L'), 1).\
    when((f.col('spend_rank') == 'L') & (f.col('penetration_rank') == 'M'), 1).\
    #Otherwise, they get nothing 0 points.
    otherwise(0)
  )

  #Give each quarter_point their weight - we want to promote recency!
  ehhn_upc_kpis = ehhn_upc_kpis.\
    join(quarters.select("quarter", "weight").dropDuplicates(), 'quarter').\
    withColumn(
      'recency_adjusted_quarter_point',
      f.round(f.col('quarter_point') * f.col('weight'), 4)
  )

  #Assign them a final and overall HML rank as a weighted average
  #of their HML rank across all quarters.
  ehhn_hml = ehhn_upc_kpis.\
    groupBy('ehhn').\
    agg(f.sum('recency_adjusted_quarter_point').alias('weighted_avg')).\
    withColumn('propensity',
              f.when(f.col('weighted_avg') > 2, 'H').\
                when(f.col('weighted_avg') > 1, 'M').\
                when(f.col('weighted_avg') <= 1, 'L').\
                otherwise('ERROR')
    )
  ehhn_hml = ehhn_hml.select('ehhn', 'propensity')

  #Rename column to match expected format - consequences of legacy code
  ehhn_hml = ehhn_hml.withColumnRenamed('propensity', 'segment')
  #Write out ehhn_upc_kpis and ehhn_hml datasets
  output_fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/funlo_lite/{}/{}_{}".format(audience_name, audience_name, today.strftime('%Y%m%d'))
  ehhn_hml.write.mode("overwrite").format("delta").save(output_fp)
  print(f"Successfully wrote out {output_fp}!")
  del(output_fp)

  #QC1: Check counts of HML ranks
  hml_counts = ehhn_hml.\
    groupBy('segment').\
    agg(f.countDistinct("ehhn").alias("ehhn_count"))
  print(hml_counts.show(50, truncate=False))
  del(hml_counts)

  #QC2: Check counts propensities used in production
  propensities = audience.propensities
  count = ehhn_hml.\
    filter(f.col('segment').isin(propensities)).\
    count()
  message = "{} ({}) ehhn counts: {}".format(audience_name, "+".join(propensities), count)
  print(message)
  del(message)
