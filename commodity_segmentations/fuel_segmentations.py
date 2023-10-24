# Databricks notebook source
## Loading External Packages
import pyspark.sql.functions as f
import pyspark.sql.types as t
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window

## Loading Internal Packages
from effodata import ACDS, golden_rules, joiner, sifter, join_on


# COMMAND ----------

#Pull one year of transaction data
end_date   = date.today().strftime("%Y-%m-%d") 
start_date = (datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=365)).strftime('%Y-%m-%d')
acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(
  start_date = start_date,
  end_date = end_date,
  apply_golden_rules=golden_rules('customer_exclusions'),
)

#Keep only fuel transactions and classify vendor label
fuel = acds.filter(f.col('wgt_fl') == '3')
#fuel = fuel.withColumn("store_number", f.col("sto_no").cast("float"))
#fuel = fuel.withColumn(
#  'vendor',
#  when((f.col("store_number")>=80000) & (f.col("store_number")<=89999), 'Shell')
#    .when((f.col("store_number")>=90000) & (f.col("store_number")<=99999), 'BP')
#    .when((f.col("store_number")>=70000) & (f.col("store_number")<=79999), 'EG Convenience')
#    .otherwise("Kroger")
#)
fuel = fuel.select( \
  f.col('ehhn'), \
  f.col('gtin_no'), \
  f.col('transactions.gtin_uom_am').alias('gallons'), \
#  f.col('store_number'), \
#  f.col('vendor') \
)
fuel = fuel.withColumn("gallons", f.col("gallons").cast("float"))
fuel = fuel.na.drop(subset=["gallons"])
#fuel = fuel.filter(f.col("vendor") == "Shell")
#fuel = fuel.select("gtin_no", "vendor", "ehhn", "gallons")

#Bump ACDS against PID to get commmodity + sub-commodity information
pid_path = "abfss://acds@sa8451posprd.dfs.core.windows.net/product/current"
pid = spark.read.parquet(pid_path)
pid = pid.select("gtin_no", "commodity_desc", "sub_commodity_desc", "con_dsc_tx")
pid = pid.dropDuplicates(["gtin_no"])
fuel = fuel.join(pid, on="gtin_no", how="inner")
#Third Party Fuel does not distinguish by fuel type (has no descriptive info from PID)
fuel = fuel.withColumn("sub_commodity_desc",
                       when(f.col("sub_commodity_desc") == "NO SUBCOMMODITY DESCRIPTION", "GASOLINE-THIRD PARTY")
                       .otherwise(f.col("sub_commodity_desc"))
)

print("Fuel Data Sample:\n")
fuel.show(25, truncate=False)
#upcs = fuel.select("gtin_no").distinct()
#upcs.show()

# COMMAND ----------

#Create a copy to do a propensity split regardless of gas type
#Third Party Fuel does not tell us fuel type - considering them altogether
#is an attempt to get value out of our third party fuel data
allfuel = fuel.alias("allfuel")
allfuel = allfuel.withColumn("sub_commodity_desc", lit("GASOLINE"))
data = fuel.union(allfuel)
data.cache()

#For each gas type (and also all of them altogether), split the households into L, M, and H propensities
gas_types = ["GASOLINE", "GASOLINE-PREMIUM UNLEADED", "GASOLINE-REG UNLEADED", "GASOLINE-UNLEADED PLUS", "GASOLINE-THIRD PARTY"]
for gas_type in gas_types:

  #Aggregate total gallons bought for each houesholds
  df = data.filter(f.col("sub_commodity_desc") == gas_type)
  df = df.groupBy("ehhn").agg(f.sum("gallons").alias("gallons"))
  print(f"Total Household Count for {gas_type}: {df.count()}")

  #Assign each houeshold to either L, M, or H depending on how much
  #gas they bought (cut-offs are the 33rd and 66th quantiles)
  #L := low, M := medium, H := High
  percentiles = df.approxQuantile("gallons", [0.33, 0.66], 0.00001)
  df = df.withColumn(
    "segmentation",
    when(f.col("gallons") < percentiles[0], "L")
        .otherwise(when(col("gallons") > percentiles[1], "H")
                  .otherwise("M"))
  )

  #To QC the methodology
  counts = df.groupBy("segmentation").count()
  avgs = df.groupBy("segmentation").agg(f.avg(f.col("gallons")).alias("average_gallons"))
  print(f"{gas_type} Counts:\n")
  counts.show()
  print(f"{gas_type} Averages:\n")
  avgs.show()

  #Write-out
  #For third party gas, take out Krogees
