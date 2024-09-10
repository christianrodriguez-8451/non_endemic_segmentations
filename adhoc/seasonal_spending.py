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

# COMMAND ----------

#Read in control file with chosen commodities + sub-commodities per holiday
fp = (
  "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" +
  "seasonal_commodities_control.csv"
)
schema = t.StructType([
    t.StructField("holiday", t.StringType(), True),
    t.StructField("commodity", t.StringType(), True),
    t.StructField("sub_commodity", t.StringType(), True),
    t.StructField("seasonality_score", t.IntegerType(), True)
])
holiday_comms = spark.read.csv(fp, schema=schema, header=True)

#Separate commodity level choices from sub-commodities level choices
h_comms = holiday_comms.\
  filter(f.col("sub_commodity") == "ALL SUB-COMMODITIES").\
  select("holiday", "commodity", "seasonality_score")
h_comms.cache()

h_scomms = holiday_comms.\
  filter(f.col("sub_commodity") != "ALL SUB-COMMODITIES").\
  select("holiday", "commodity", "sub_commodity", "seasonality_score")
h_scomms = h_scomms.withColumnRenamed("seasonality_score", "seasonality_sub_score")
h_scomms.cache()

h_comms.show(10, truncate=False)
h_scomms.show(10, truncate=False)

# COMMAND ----------

#Read in PIM and keep product name, product description, commodity,
#sub-commodity, department, and sub-department.
pim_fp = config.get_latest_modified_directory(config.azure_pim_core_by_cycle)
pim = config.spark.read.parquet(pim_fp)
pim = pim.select(
  f.col("upc").alias("gtin_no"),
  f.col("gtinName").alias("product_name"),
  f.col("krogerOwnedEcommerceDescription").alias("product_description"),
  f.col("familyTree.commodity.name").alias("commodity"),
  f.col("familyTree.subCommodity.name").alias("sub_commodity"),
  f.col("familyTree.primaryDepartment.name").alias("department"),
  f.col("familyTree.primaryDepartment.recapDepartment.name").alias("sub_department"),
  f.col("familyTree.primaryDepartment.recapDepartment.department.name").alias("micro_department"),
)

#Put aside department info - for tracking department spending over time
department_info = pim.\
  select("commodity", "sub_commodity", "department", "sub_department", "micro_department").\
  dropDuplicates(["commodity", "sub_commodity"])
department_info.cache()

#For assigning each gtin_no their commodity and sub-commodity
pim = pim.select("gtin_no", "commodity", "sub_commodity")
pim = pim.dropDuplicates(["gtin_no"])
pim.cache()

# COMMAND ----------

#Define start and end dates for ACDS pull
start_date_str = "20230101"
end_date_str = "20240102"

#Convert to datetime objects for ACDS
start_date = datetime.strptime(start_date_str, '%Y%m%d')
end_date = datetime.strptime(end_date_str, '%Y%m%d')

#Pull in transaction data from 2023-01-02 to 2024-01-02
#Pull in transaction data and keep only ehhn, gtin_no, and dollars_spent
acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))
acds = acds.select("ehhn", "gtin_no", "net_spend_amt", "scn_unt_qy", f.col("trn_dt").alias("date"))
#Drop returns
acds = acds.filter(f.col("net_spend_amt") > 0)
acds.cache()

acds.show(truncate=False)

# COMMAND ----------

#Define holiday dates
h_dates = {
  #"super_bowl": "20230212", #Done
  #"valentines_day": "20230214", #Done
  #"st_patricks_day": "20230317", #Done
  "easter": "20230409", #Note done yet!!!
  #"may_5th": "20230505", #Done
  #"mothers_day": "20230514", #Done
  #"memorial_day": "20230529", #Done
  #"fathers_day": "20230618", #Done
  #"july_4th": "20230704", #Done
  "back_to_school": "20230819", #Done???
  #"labor_day": "20230904", #Done
  #"halloween": "20231031", #Done
  "thanksgiving": "20231123", #Done???
  #"christmas_eve": "20231224", #Done
  #"new_years_eve": "20231231", #Done
}

#For each holiday, get distribution of spending on the day level. Pull 30 days of spending for each holiday.
for h in list(h_dates.keys()):
  #Pull spending on holiday + 30 days back
  h_enddate = h_dates[h]
  h_enddate = datetime.strptime(h_enddate, '%Y%m%d')
  h_startdate = h_enddate - timedelta(days=30)
  h_trans = acds.filter((f.col("date") >= int(h_startdate.strftime('%Y%m%d'))) & (f.col("date") <= int(h_enddate.strftime('%Y%m%d'))))

  #Assign commodity, sub-commodity, and seasonality_score to each gtin_no
  h_pim1 = h_comms.\
    filter(f.col("holiday") == h).\
    select("commodity", "seasonality_score").\
    join(pim.select("gtin_no", "commodity", "sub_commodity"),
        "commodity",
        "inner").\
    dropDuplicates(["gtin_no"])
  h_pim2 = h_scomms.\
    filter(f.col("holiday") == h).\
    select("commodity", "sub_commodity", "seasonality_sub_score").\
    join(pim.select("gtin_no", "commodity", "sub_commodity"),
        ["commodity", "sub_commodity"],
        "inner").\
    dropDuplicates(["gtin_no"])
  h_pim = h_pim1.join(h_pim2,
                      ["gtin_no", "commodity", "sub_commodity"],
                      "outer")
  #Special rule: if there is an assigned seasonality score at the commodity level and there is an assigned
  #seasonality score for a sub-commodity within that same commodity, then the sub-commodity level
  #seasonality score triumps the commodity level seasonality score
  #when sub-commodity seasonality score > commodity seasonality score
  h_pim = h_pim.fillna({"seasonality_score": 0, "seasonality_sub_score": 0})
  h_pim = h_pim.withColumn(
    "seasonality_score",
    f.when(f.col("seasonality_sub_score") > f.col("seasonality_score"), f.col("seasonality_sub_score")).\
      otherwise(f.col("seasonality_score"))
  )
  h_pim = h_pim.orderBy(["gtin_no", "seasonality_score"], ascending=[True, False])
  h_pim = h_pim.select("gtin_no", "commodity", "sub_commodity", "seasonality_score")
  h_pim = h_pim.dropDuplicates(["gtin_no"])
  #Assign department level info - this will be nice to see when analyzing spending over time
  h_pim = h_pim.join(department_info,
                    ["commodity", "sub_commodity"],
                    "left")
  #Keep only transactions that involved chosen commodities and sub-commodities
  h_trans = h_trans.join(h_pim, "gtin_no", "inner")
  del(h_pim1, h_pim2, h_pim)

  #Calculated weighted spending: (seasonal_weight + temporal weight) * net_spend_amt
  h_trans = h_trans.withColumn("weighted_net_spend_amt", f.col("seasonality_score") * f.col("net_spend_amt"))
  #Mute transactions x days away
  weighted_startdate = h_enddate - timedelta(days=6)
  h_trans = h_trans.withColumn(
    "weighted_net_spend_amt",
    f.when(f.col("date") < int(weighted_startdate.strftime('%Y%m%d')), 0).otherwise(f.col("weighted_net_spend_amt")))

  #Sum up spending at deparment/daily/household level to see spending over time at department level
  h_department = h_trans.\
    groupby("micro_department", "date", "ehhn").\
    agg(
      f.sum("net_spend_amt").alias("dollars_spent"),
      f.sum("weighted_net_spend_amt").alias("weighted_dollars_spent"),
    )
  #Sum up spending at daily/household level to see spending over time
  h_daily = h_department.\
    groupby("date", "ehhn").\
    agg(
      f.sum("dollars_spent").alias("dollars_spent"),
      f.sum("weighted_dollars_spent").alias("weighted_dollars_spent"),
    )
  #Sum up spending for each household
  h_ehhns = h_daily.\
    groupby("ehhn").\
    agg(
      f.sum("dollars_spent").alias("dollars_spent"),
      f.sum("weighted_dollars_spent").alias("weighted_dollars_spent"),
    )
  del(h_startdate, h_enddate, h_trans)

  #Write-out
  output_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/" + h + "/"
  fp = output_dir + "{}_microdepartment_spending".format(h)
  h_department.write.mode("overwrite").format("delta").save(fp)
  fp = output_dir + "{}_daily_spending".format(h)
  h_daily.write.mode("overwrite").format("delta").save(fp)
  fp = output_dir + "{}_ehhn_spending".format(h)
  h_ehhns.write.mode("overwrite").format("delta").save(fp)
  del(h_department, h_daily, h_ehhns)
