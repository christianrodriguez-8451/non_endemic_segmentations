# Databricks notebook source
#When you use a job to run your notebook, you will need the service principles
#You only need to define what storage accounts you are using
#Define service principals
service_credential = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-pw')
service_application_id = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-app-id')
directory_id = "5f9dc6bd-f38a-454a-864c-c803691193c5"
#storage_account = 'sa8451entlakegrnprd'
#storage_account = 'sa8451dbxadhocprd'
storage_account = 'sa8451posprd'

#Set configurations
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

import dateutil.relativedelta as dr
import datetime as dt
from effodata import ACDS, golden_rules
import pyspark.sql.functions as f
import pandas as pd

#Read in your control file (for each segment, specifies which commodities + sub-commodities it is made of)
comm_dir = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/commodity_segments'
con_fn = "commodity_segments_control.csv"
con_fp = comm_dir + "/" + con_fn
segments_df = spark.read.csv(con_fp, sep=",", header=True)
segments_df = segments_df.toPandas()
segments_df = segments_df.loc[~segments_df["segmentation"].isna(), :]

#Turn control file into dictionary for easier handling
segments_dict = {}
for n in range(0, len(list(segments_df["segmentation"]))):
  
  #Column order is assumed to be segment, commodities, sub-commodities, weeks.
  segment = segments_df.iat[n, 0]
  segment = segment.strip()
  segment = segment.lower()
  segment = segment.replace(" ", "_")
  segment = segment.replace("+", "and")

  #The columns with lists of strings are delimited by pipe and
  #commodities/sub-commodities are capitalized within PID.
  str_delim = "|"

  commodities = segments_df.iat[n, 1]
  if not commodities == None:
    commodities = commodities.split(str_delim)
    commodities = [c.strip() for c in commodities]
    commodities = [c.upper() for c in commodities]
  else:
    commodities = []

  sub_commodities = segments_df.iat[n, 2]
  if not sub_commodities == None:
    sub_commodities = sub_commodities.split(str_delim)
    sub_commodities = [c.strip("") for c in sub_commodities]
    sub_commodities = [c.upper() for c in sub_commodities]
  else:
    sub_commodities = []

  weeks =  int(segments_df.iat[n, 3])

  segments_dict[segment] = {
    "commodities": commodities,
    "sub_commodities": sub_commodities,
    "weeks": weeks,
  }
  del(segment, commodities, sub_commodities)

#Define the time range of data that you are pulling
max_weeks = 52
today = dt.date.today()
last_monday = today - dr.datetime.timedelta(days=today.weekday())
start_date = last_monday - dr.datetime.timedelta(weeks=max_weeks)
end_date = last_monday - dr.datetime.timedelta(days=1)

#ACDS2 = used to extract household counts
acds2 = ACDS(use_sample_mart=False)
acds2 = acds2.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))
acds2 = acds2.select("gtin_no", "ehhn")
acds2 = acds2.dropDuplicates()
acds2.cache()

#Pull in transaction data
acds = ACDS(use_sample_mart=False)
acds = acds.get_transactions(start_date, end_date, apply_golden_rules=golden_rules(['customer_exclusions']))
#Add in a week column in your acds pull to enable easy querying of variable weeks
acds = acds.\
withColumn('year', f.substring('trn_dt', 1, 4)).\
withColumn('month', f.substring('trn_dt', 5, 2)).\
withColumn('day', f.substring('trn_dt', 7, 2))
acds = acds.withColumn('date', f.concat(f.col('month'), f.lit("-"), f.col("day"), f.lit("-"), f.col("year")))
acds = acds.select("gtin_no", "trn_dt", f.to_date(f.col("date"), "MM-dd-yyyy").alias("transaction_date"))
acds = acds.withColumn("end_date", f.lit(end_date))
acds = acds.select("gtin_no", "trn_dt", "transaction_date", "end_date",
                   f.datediff(f.col("end_date"), f.col("transaction_date")).alias("day"))
acds = acds.withColumn("week", f.col("day")/7)
acds = acds.withColumn("week", f.ceil(f.col("week")))
acds = acds.select("gtin_no", "week")
acds = acds.dropDuplicates()

#Bump ACDS against PID to get commmodity + sub-commodity information
pid_path = "abfss://acds@sa8451posprd.dfs.core.windows.net/product/current"
pid = spark.read.parquet(pid_path)
pid = pid.select("gtin_no", "commodity_desc", "sub_commodity_desc")
pid = pid.dropDuplicates(["gtin_no"])
acds = acds.join(pid, on="gtin_no", how="inner")

#Filter for only the relevant commodities + sub-commodities to keep the dataframe small
my_comms = []
my_subs = []
for s in segments_dict:
  my_comms += segments_dict[s]["commodities"]
  my_subs += segments_dict[s]["sub_commodities"]

my_comms = list(set(my_comms))
my_subs = list(set(my_subs))
all_gtins = acds.filter((acds["commodity_desc"].isin(my_comms)) | (acds["sub_commodity_desc"].isin(my_subs)))
all_gtins.cache()
del(my_comms, my_subs)

for s in list(segments_dict.keys()):
  #Un-pack the parameters for the given segment
  my_dict = segments_dict[s]
  comms = my_dict["commodities"]
  sub_comms = my_dict["sub_commodities"]
  n_weeks = my_dict["weeks"]

  #Filter only on the commodities + sub-commodities relevant to the given segment
  segment_gtins = all_gtins.filter((all_gtins["commodity_desc"].isin(comms)) | (all_gtins["sub_commodity_desc"].isin(sub_comms)))
  segment_gtins = segment_gtins.filter(segment_gtins["week"] <= n_weeks)
  segment_gtins = segment_gtins.select("gtin_no")
  segment_gtins = segment_gtins.dropDuplicates()

  #Write-out the file
  cyc_date = dt.date.today().strftime('%Y-%m-%d')
  output_dir = comm_dir + "/" + "output" + "/" + "cycle_date={}".format(cyc_date)
  if not ("cycle_date={}".format(cyc_date) in list(dbutils.fs.ls(comm_dir + "/" + "output"))):
    dbutils.fs.mkdirs(output_dir)

  output_fn = "{}_{}".format(s, cyc_date)
  output_fp =  output_dir + "/" + output_fn
  segment_gtins.write.mode("overwrite").format("delta").save(output_fp)
  segment_gtins.cache()

  #The egress directory where the files are FTP'd from Azure to On-Premise
  embedded_dimensions_dir = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions'
  egress_dir = embedded_dimensions_dir + '/egress/upc_list'

  #Used to process the UPC list into a format that the engineering team can take in
  def create_upc_json(df, query):
    from pyspark.sql.functions import collect_list
    upc_list = df.rdd.map(lambda column: column.gtin_no).collect()
    upc_string = '","'.join(upc_list)
    
    upc_format = '{"cells":[{"order":0,"type":"BUYERS_OF_PRODUCT","purchasingPeriod":{"startDate":"'+ str(start_date) +'","endDate":"'+ str(end_date) +'","duration":52},"cellRefresh":"DYNAMIC","behaviorType":"BUYERS_OF_X","xProducts":{"upcs":["'+ upc_string +'"]},"purchasingModality":"ALL"}],"name":"'+ query +'","description":"Buyers of '+ query +' products."}'
    return upc_format

  #Copy the file from outputted location to egress pick-up location
  json_payload = create_upc_json(segment_gtins, s)
  rdd = spark.sparkContext.parallelize([json_payload])
  df = spark.read.json(rdd)
  #df.coalesce(1).write.mode("overwrite").json(output_fp)
  #dbutils.fs.cp(output_fp, egress_dir +'/' + output_fn + '.json')

  #print(segment_gtins.show(5, truncate=False))
  hh_count = acds2.join(segment_gtins, on="gtin_no", how="inner")
  hh_count = hh_count.count()
  print("{} - household count {}!".format(s, hh_count))

  del(comms, sub_comms, n_weeks, segment_gtins)
