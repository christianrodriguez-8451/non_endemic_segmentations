# Databricks notebook source
"""
Creates the travelers and roadies segmentations by looking at the last
52 weeks of transaction data.

Travelers are households that were found on a significant trip.
A significant trip is defined as a household making a purchase at
a store that is at least 250km away from their preferred store.
Within travelers, a household was given H propensity if they
went on 2 unique significant trips. 

Roadies are households that satisfy one of two conditions:
  
  1) The household was on a significant trip and they were
  found to be purchasing fuel.

  2) The houeshold is in the top 90th percentile of
  all-inclusive fuel gallon purchasers.

Within roadies, a household was given H propensity if they
satisfied both conditions. Otherwise, they were given a 
M propensity.
"""

# COMMAND ----------

#When you use a job to run your notebook, you will need the service principles
#You only need to define what storage accounts you are using

#Define service principals
service_credential = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-pw')
service_application_id = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-app-id')
directory_id = "5f9dc6bd-f38a-454a-864c-c803691193c5"
storage_account = 'sa8451dbxadhocprd'
#storage_account = 'sa8451posprd'

#Set configurations
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

#For dataframe manipulations
import pyspark.sql.functions as f
import pyspark.sql.types as t
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
#Load internal packages to read in preffered stores and ACDS
from effodata import ACDS, golden_rules, joiner, sifter, join_on
import seg
from seg.utils import DateType
#To calculate Haversine distance
from math import radians, sin, cos, sqrt, atan2
from pyspark.sql import SparkSession
#To plot histograms
import matplotlib.pyplot as plt
import pandas as pd

# COMMAND ----------

#Pull one year of transaction data
#today = "20231117"
today = date.today().strftime('%Y%m%d')
end_date   = (datetime.strptime(today, '%Y%m%d') - timedelta(days=7)).strftime('%Y%m%d')
start_date = (datetime.strptime(today, '%Y%m%d') - timedelta(days=365)).strftime('%Y%m%d')
acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(
  start_date = start_date,
  end_date = end_date,
  apply_golden_rules=golden_rules('customer_exclusions'),
)
acds = acds.select(
  "ehhn",
  "transaction_code",
  "gtin_no",
  "net_spend_amt",
  f.col('transactions.gtin_uom_am').alias('gallons'),
  "wgt_fl",
  f.col("mgt_div_no").alias("div_no"),
  "sto_no",
)
#Create store code to merge against preferred store and store DNA databases
acds = acds.withColumn("store_code", f.concat(f.col("div_no"), f.lit("_"), f.col("sto_no")))
acds = acds.drop('div_no', 'sto_no', 'raw_date')
acds.cache()

#ehhn_trips = acds.select("ehhn", "week", "transaction_code", "store_code")
ehhn_trips = acds.select("ehhn", "transaction_code", "store_code")
ehhn_trips = ehhn_trips.dropDuplicates()

#Read in prefered store database
today = datetime.now().strftime("%Y%m%d")
preferred_store = seg.get_seg_for_date(seg="pref_store", date=today)
preferred_store = preferred_store.select(
  "ehhn",
  f.col("pref_store_code_1").alias("pref_store_code"),
)

#Read in store DNA to get longitude/latitude on each store
store_dna = spark.read.format("delta").load("abfss://geospatial@sa8451geodev.dfs.core.windows.net/SDNA/GSC_SDNA")
store_dna = store_dna.select(
  f.col("STORE_CODE").alias("store_code"),
  f.col("LONGITUDE").alias("longitude"),
  f.col("LATITUDE").alias("latitude"),
)
store_dna = store_dna.dropDuplicates()

#print("Number of entries in ehhn_trips before merging with Store DNA: {}".format(ehhn_trips.count()))
ehhn_trips = ehhn_trips.join(
  store_dna,
  on="store_code",
  how="inner",
)
#print("Number of entries in ehhn_trips after merging with Store DNA: {}".format(ehhn_trips.count()))
#print("Number of entries in Preferred Store before merging with Store DNA: {}".format(preferred_store.count()))
preferred_store = preferred_store.join(
  store_dna,
  preferred_store["pref_store_code"] == store_dna["store_code"],
  how="inner",
)
#print("Number of entries in Preferred Store after merging with Store DNA: {}".format(preferred_store.count()))
preferred_store = preferred_store.withColumnRenamed('longitude', 'pref_longitude')
preferred_store = preferred_store.withColumnRenamed('latitude', 'pref_latitude')
preferred_store = preferred_store.select(
  "ehhn",
  "pref_store_code",
  "pref_longitude",
  "pref_latitude",
)
del(store_dna)

#Merge and only keep entries where prefered store != transaction store
ehhn_trips = ehhn_trips.join(
  preferred_store,
  on="ehhn",
  how="inner",
)
ehhn_trips = ehhn_trips.filter(ehhn_trips["pref_store_code"] != ehhn_trips["store_code"])
#print("Number of entries in ehhn_trips after merging with Preferred Store and keeping only non-Preferred entries: {}".format(ehhn_trips.count()))

# COMMAND ----------

#Calculate distance (in km) between prefered store & transaction stores
@f.udf(returnType=t.DoubleType())
def haversine_distance(lat1, lon1, lat2, lon2):
    # Radius of the Earth in kilometers
    R = 6371.0

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c

    return distance

spark = SparkSession.builder.appName("HaversineDistance").getOrCreate()
ehhn_trips = ehhn_trips.withColumn("longitude", f.col("longitude").cast("float"))
ehhn_trips = ehhn_trips.withColumn("pref_longitude", f.col("pref_longitude").cast("float"))
ehhn_trips = ehhn_trips.withColumn("latitude", f.col("latitude").cast("float"))
ehhn_trips = ehhn_trips.withColumn("pref_latitude", f.col("pref_latitude").cast("float"))
ehhn_trips = ehhn_trips.withColumn(
  "haversine_distance",
  haversine_distance(
    ehhn_trips["pref_latitude"],
    ehhn_trips["pref_longitude"],
    ehhn_trips["latitude"],
    ehhn_trips["longitude"],
  ),
)
ehhn_trips = ehhn_trips.dropDuplicates(["transaction_code"])
ehhn_trips.cache()

print(f"Data sample of ehhn_trips: \n{ehhn_trips.show(25, truncate=False)}\n")

# COMMAND ----------

#250km is the cut-off for a signficant trip
#250km is about 3 hours of driving time to get from Chicago to Indianapolis
threshold = 250
#Assign significant distance flag based on threshold
ehhn_trips = ehhn_trips.withColumn("significant_distance", f.when(f.col("haversine_distance") >= threshold, 1).otherwise(0))

#Get a count of how many times the houeshold took a significant trip
trip_counts = ehhn_trips.groupBy("ehhn").agg(f.sum(f.col("significant_distance")).alias("significant_trips"))
print("\nSummary statistics for threshold {}km:\n".format(threshold))
print(trip_counts.describe("significant_trips").show())

cut_off = trip_counts.approxQuantile("significant_trips", [0.95], 0.01)
trip_counts = trip_counts.filter(trip_counts["significant_trips"] <= cut_off[0])

#See histogram plot of trips
col_to_plot = "significant_trips"
pandas_df = trip_counts.select(col_to_plot).toPandas()
plt.hist(pandas_df[col_to_plot], bins=20, edgecolor='black')
plt.xlabel(col_to_plot)
plt.ylabel("Frequency")
plt.title("Histogram of " + col_to_plot)
print(plt.show())

#Get a count of how many unique significant trips the household took
unique_trip_counts = ehhn_trips.dropDuplicates(["ehhn", "store_code"])
unique_trip_counts = unique_trip_counts.groupBy("ehhn").agg(f.sum(f.col("significant_distance")).alias("unique_significant_trips"))
print(unique_trip_counts.describe("unique_significant_trips").show())

cut_off = unique_trip_counts.approxQuantile("unique_significant_trips", [0.95], 0.01)
unique_trip_counts = unique_trip_counts.filter(unique_trip_counts["unique_significant_trips"] <= cut_off[0])

#See histogram plot of trips
col_to_plot = "unique_significant_trips"
pandas_df = unique_trip_counts.select(col_to_plot).toPandas()
plt.hist(pandas_df[col_to_plot], bins=20, edgecolor='black')
plt.xlabel(col_to_plot)
plt.ylabel("Frequency")
plt.title("Histogram of " + col_to_plot)
print(plt.show())

# COMMAND ----------

#Anybody who has traveled at least one significant trip is considered a traveler!
travelers = unique_trip_counts.filter(unique_trip_counts["unique_significant_trips"] >= 1)
travelers = travelers.select("ehhn", "unique_significant_trips")
travelers = travelers.dropDuplicates()
travelers.cache()

print(f"Count of households found on a significant trip (travelers): {travelers.count()}")

#Roadie condition 1 - found purchasing fuel on a significant trip
#Merge the travelers against their trips to ge their complete history of significant trips
roadies1 = travelers.select("ehhn")
roadies1 = roadies1.join(ehhn_trips.select("ehhn", "store_code", "transaction_code", "significant_distance"),
                        on=["ehhn"],
                        how="inner")
roadies1 = roadies1.filter(roadies1["significant_distance"] == 1)
#Get all distinct fuel transactions
temp = acds.select("ehhn", "transaction_code", "wgt_fl")
temp = temp.filter(temp["wgt_fl"] == "3")
temp = temp.dropDuplicates(["transaction_code"])
temp = temp.select("transaction_code")
#For all of the designated travelers, keep the ones purchasing gas on their significant trip
roadies1 = roadies1.join(temp.select("transaction_code"),
                         on="transaction_code",
                         how="inner")
roadies1 = roadies1.select(["ehhn"]).dropDuplicates()
roadies1.cache()

print(f"Count of households found purchasing fuel while on a significant trip: {roadies1.count()}")

# COMMAND ----------

from commodity_segmentations.config import output_fp as fuel_fp

#Roadie condition 2 - in top 90th percentile of fuel purchases
#Read in segmentation file for all-inclusive fuel segmentation
fuel_fp = fuel_fp + "fuel_segmentations/gasoline/gasoline_20231110"
fuel = spark.read.format("delta").load(fuel_fp)

#Keep only households in top 90th percentile of fuel purchases
cut_off = fuel.approxQuantile("gallons", [0.90], 0.01)
cut_off = cut_off[0]
roadies2 = fuel.filter(fuel["gallons"] >= cut_off)
roadies2 = roadies2.select("ehhn").dropDuplicates()

print(f"90th percentile cut-off for household fuel gallon purchases: {cut_off}")
print(f"Count of households in top 90th percentile of all fuel gallon purchases: {roadies2.count()}")

# COMMAND ----------

#See all roadies that satisfied both conditions!
super_roadies = roadies1.join(roadies2,
                              on="ehhn",
                              how="inner")
super_roadies.cache()

print(f"Count of households found purchasing fuel on a significant trip AND in top 90th percentile: {super_roadies.count()}")

# COMMAND ----------

#Create and out the segmentation files for travelers and roadies
#Travelers - H for households who went on atleast 2 unique significant trips,
#            M for households who went on 1 unique significant trip
travelers = travelers.withColumn(
  "segment",
  f.when(f.col("unique_significant_trips") >= 2, "H").otherwise("M")
)
travelers = travelers.select("ehhn", "segment")
#travelers.show(5, truncate=False)
output_fp = f"abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/geospatial/travelers/travelers_{today}"
travelers.write.mode("overwrite").format("delta").save(output_fp)

#Roadies - H for households that are super roadies
#          M for households that satisfy one of the two conditions
super_roadies = super_roadies.select("ehhn").dropDuplicates()
super_roadies = super_roadies.withColumn("segment", f.lit("H"))

roadies = roadies1.select("ehhn")
roadies = roadies.union(roadies2.select("ehhn"))
roadies = roadies.dropDuplicates()
roadies = roadies.join(super_roadies, on="ehhn", how="left_outer")
roadies = roadies.withColumn("segment", f.lit("M"))

roadies = super_roadies.union(roadies)
#roadies.show(5, truncate=False)
output_fp = f"abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/geospatial/roadies/roadies_{today}"
roadies.write.mode("overwrite").format("delta").save(output_fp)
