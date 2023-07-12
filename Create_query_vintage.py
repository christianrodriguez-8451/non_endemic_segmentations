# Databricks notebook source
spark.conf.set("spark.sql.shuffle.partitions","auto")
from pyspark.sql.functions import collect_list
from pathlib import Path

# COMMAND ----------

#Define service principals
service_credential = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-pw')
service_application_id = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-app-id')
directory_id = "5f9dc6bd-f38a-454a-864c-c803691193c5"
storage_account = 'sa8451dbxadhocprd'

# COMMAND ----------

#Set configurations
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

#Define service principals
service_credential = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-pw')
service_application_id = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-app-id')
directory_id = "5f9dc6bd-f38a-454a-864c-c803691193c5"
storage_account = 'sa8451posprd'

# COMMAND ----------

#Set configurations
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

def get_latest_modified_directory(pDirectory):
    """
    get_latest_modified_file_from_directory:
        For a given path to a directory in the data lake, return the directory that was last modified. 
        Input path format expectation: 'abfss://x@sa8451x.dfs.core.windows.net/
    """
    #Set to get a list of all folders in this directory and the last modified date time of each
    vDirectoryContentsList = list(dbutils.fs.ls(pDirectory))

    #Convert the list returned from get_dir_content into a dataframe so we can manipulate the data easily. Provide it with column headings. 
    #You can alternatively sort the list by LastModifiedDateTime and get the top record as well. 
    df = spark.createDataFrame(vDirectoryContentsList,['FullFilePath', 'LastModifiedDateTime'])

    #Get the latest modified date time scalar value
    maxLatestModifiedDateTime = df.agg({"LastModifiedDateTime": "max"}).collect()[0][0]

    #Filter the data frame to the record with the latest modified date time value retrieved
    df_filtered = df.filter(df.LastModifiedDateTime == maxLatestModifiedDateTime)
    
    #return the file name that was last modifed in the given directory
    return df_filtered.first()['FullFilePath']

embedded_dimensions_dir = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions'
product_vectors_dir = '/product_vectors_diet_description/cycle_date='
diet_query_dir = '/diet_query_embeddings'
vintages_dir = '/customer_data_assets/vintages'

dbutils.widgets.text("upc_list_path_api", "")
upc_list_path = dbutils.widgets.get("upc_list_path_api")

# Set path for product vectors
if upc_list_path_api:
  diet_query_embeddings_directories_list = [Path(upc_list_path_api).parts[-1]]
  data = [{"path": upc_list_path_api}]
  upc_list_path_lookup = spark.createDataFrame(data)
else:
  upc_list_path = get_latest_modified_directory(embedded_dimensions_dir + diet_query_dir) 
  diet_query_embeddings_directories = spark.createDataFrame(list(dbutils.fs.ls(upc_list_path)))
  upc_list_path_lookup = spark.createDataFrame(list(dbutils.fs.ls(upc_list_path)))
  diet_query_embeddings_directories_list = diet_query_embeddings_directories.rdd.map(lambda column: column.name).collect()

# get current datetime
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

# minus 1 year
yesterday = date.today() - timedelta(days=1)
today_year_ago = (yesterday - relativedelta(years=1)).strftime('%Y%m%d')
today = datetime.today().strftime('%Y%m%d')

# COMMAND ----------

from effodata import golden_rules, Joiner, Sifter, Equality, ACDS
from kpi_metrics import KPI
import pyspark.sql.functions as f

# COMMAND ----------

##I will have 3 append process files
#IPD = Instore, Pickup or Delivery. The KPI aggregate query will filter based on the modality field, all Golden Rules will apply
#S = Ship, will be a different function entirely (ACDS building, plans to integrate at end of December). May have other things to change based on functionality. won't create until then
#Ocado = will have to remove store exclusions from golden rules parameter as 540 is removed by default. also will have a different filter and parameters will look a little different (Division filter will only be 540). if we want at a shed level, this is more complex and need to investigate. 

# COMMAND ----------
for directory_name in diet_query_embeddings_directories_list:
    try:
      dbutils.fs.ls(embedded_dimensions_dir + vintages_dir + "/hh_" + directory_name)
      print(embedded_dimensions_dir + vintages_dir + "/hh_" + directory_name + "exists and DOESN'T need to be created")
    except:
      print(embedded_dimensions_dir + vintages_dir + "/hh_" + directory_name + " doesn't exist and needs to be created")
      
      if upc_list_path_api:
        upc_vectors_path = upc_list_path_api
      else:
        upc_vectors_path = upc_list_path + directory_name   
        
      upc_vectors_dot_product = spark.read.format("delta").load(upc_vectors_path)
      
      directory_name = directory_name.replace('/', '')

      fiscal_st_dt = today_year_ago
      fiscal_end_dt = today 
      modality_acds = directory_name 
      modality_name = modality_acds.lower()

      acds = ACDS(use_sample_mart=False)
      kpi = KPI(use_sample_mart=False)
      dates_tbl = acds.dates.select('trn_dt', 'fiscal_week', 'fiscal_month', 'fiscal_quarter', 'fiscal_year')

      df_ecomm_baskets = kpi.get_aggregate(
      start_date = fiscal_st_dt,
      end_date = fiscal_end_dt,
      metrics = ["sales","gr_visits","units"],
      join_with = 'stores', #need this to use geo
      apply_golden_rules = golden_rules(), #will need to remove store exclusions GR for Ocado and Ship (Vitacost as Division). this by default will include ["011","014","016","018","021","024","025","026","029","034","035","105","531","534","615","620","660","701","703","705","706"] per https://github.8451.com/FoundationalComponents/GoldenRules/blob/master/Store_Exclusions.md
      group_by = ["ehhn","trn_dt","geo_div_no"], 
      filter_by=Sifter(upc_vectors_dot_product, join_cond=Equality("gtin_no"), method="include")
      )

      pickup_bsk = df_ecomm_baskets.select('ehhn', 'trn_dt', 'sales', 'units', 'gr_visits').filter(df_ecomm_baskets.ehhn.isNotNull())

      trans_agg = pickup_bsk.join(dates_tbl, 'trn_dt', 'inner')\
                            .groupBy('ehhn', 'fiscal_week', 'fiscal_month', 'fiscal_quarter', 'fiscal_year')\
                            .agg(f.sum('sales').alias('weekly_sales'),
                                f.sum('units').alias('weekly_units'),
                                f.sum('gr_visits').alias('weekly_visits'))

      #finding division 
      pickup_bsk_div = df_ecomm_baskets.select('ehhn', 'trn_dt', 'sales', 'geo_div_no').filter(df_ecomm_baskets.ehhn.isNotNull())
      ehhn_min_dt = pickup_bsk_div.groupBy('ehhn').agg(f.min('trn_dt').alias('trn_dt'))
      ehhn_div_min = pickup_bsk_div.join(ehhn_min_dt, how = 'inner', on = ['ehhn','trn_dt'])

      div_agg = ehhn_div_min.join(dates_tbl, 'trn_dt', 'inner').orderBy('trn_dt').groupBy('ehhn').agg(f.first('geo_div_no').alias('vintage_div'),f.min('fiscal_week').alias('fiscal_week'))

      new_hhs = trans_agg.where(f.col('weekly_visits')>0)
      hhs_min_week = new_hhs.groupBy('ehhn')\
                              .agg(f.min('fiscal_week').alias('fiscal_week'))

      hh_vintage = hhs_min_week.join(dates_tbl, 'fiscal_week', 'inner') \
      .join(div_agg,how = 'inner', on = ['ehhn','fiscal_week']) \
      .select('ehhn', 'fiscal_week', 'fiscal_year', 'fiscal_month', 'fiscal_quarter','vintage_div')\
      .distinct()\
      .withColumnRenamed('fiscal_week', 'vintage_week')\
      .withColumnRenamed('fiscal_year', 'vintage_year')\
      .withColumnRenamed('fiscal_month', 'vintage_period')\
      .withColumnRenamed('fiscal_quarter', 'vintage_quarter')

      #tried making this an overwrite but then it deleted all weeks and overwrites the entire folder
      hh_vintage.coalesce(1).write.mode('overwrite').partitionBy('vintage_week').parquet(embedded_dimensions_dir + vintages_dir + '/hh_' + modality_name)

      if upc_list_path_api:
        ##write look up file of upc list location 
        upc_list_path_lookup.write.mode("overwrite").format("delta").save(embedded_dimensions_dir + vintages_dir + '/hh_' + directory_name + '/upc_list_path_lookup')
      else:
        ##write look up file of upc list location 
        directory_name_slash = directory_name + '/'
        upc_list_path_lookup.select(upc_list_path_lookup.path,upc_list_path_lookup.name).where(upc_list_path_lookup.name == directory_name_slash).write.mode("overwrite").format("delta").save(embedded_dimensions_dir + vintages_dir + '/hh_' + directory_name + '/upc_list_path_lookup') 
      
      new_hh_vintage = spark.read.parquet(embedded_dimensions_dir + vintages_dir + '/hh_' + modality_name)
      trans_agg_vintage = trans_agg.join(new_hh_vintage, 'ehhn', 'inner')

      trans_agg_vintage.coalesce(1).write.mode('overwrite').partitionBy('fiscal_week').parquet(embedded_dimensions_dir + vintages_dir + '/sales_' + modality_name)
