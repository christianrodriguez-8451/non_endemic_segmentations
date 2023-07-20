# Databricks notebook source
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

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from effodata import golden_rules, Joiner, Sifter, Equality, ACDS
import argparse
from datetime import datetime
from kpi_metrics import KPI
from pathlib import Path

# COMMAND ----------

acds = ACDS(use_sample_mart=False)
kpi = KPI(use_sample_mart=False)
dates_tbl = acds.dates.select('trn_dt', 'fiscal_week', 'fiscal_month', 'fiscal_quarter', 'fiscal_year')

# COMMAND ----------

import kayday as kd

def get_fw(weeks_ago):
    today = kd.KrogerDate(date='today')
    weeks_ago_today = today.ago(weeks=weeks_ago)
    weeks_ago_today_week = weeks_ago_today.format_week()[1:]
    return weeks_ago_today_week

def get_start_date(weeks_ago):
    today = kd.KrogerDate(date='today')
    weeks_ago_today = today.ago(weeks=weeks_ago)
    weeks_ago_today_week = kd.DateRange(weeks_ago_today.format_week())
    week_start_date = weeks_ago_today_week.start_date.format_cal_date()
    return week_start_date
  
def get_end_date(weeks_ago):
    today = kd.KrogerDate(date='today')
    weeks_ago_today = today.ago(weeks=weeks_ago)
    weeks_ago_today_week = kd.DateRange(weeks_ago_today.format_week())
    week_end_date = weeks_ago_today_week.end_date.format_cal_date()
    return week_end_date

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

# COMMAND ----------

diet_query_vintages_directories = spark.createDataFrame(list(dbutils.fs.ls(embedded_dimensions_dir + vintages_dir)))
diet_query_vintages_directories = diet_query_vintages_directories.filter(diet_query_vintages_directories.name.like('sales_%'))
diet_query_vintages_directories = diet_query_vintages_directories.rdd.map(lambda column: column.name).collect()
modality_list_nonship = diet_query_vintages_directories
fw = get_fw(1)
start_date = get_start_date(1)
end_date = get_end_date(1)

# COMMAND ----------

for modality_name in modality_list_nonship: 
    modality_name = modality_name.replace('sales_', '')
    modality_name = modality_name.replace('/', '')
    #Pull existing vintage hh and sales data for the modality
    og_hh_vintage = spark.read.option("mergeSchema", "true").option("basePath", f'{embedded_dimensions_dir}{vintages_dir}/hh_{modality_name}').parquet(f'{embedded_dimensions_dir}{vintages_dir}/hh_{modality_name}/v*').where(f.col('vintage_week')<fw)
    
    og_trans_agg_vintage = spark.read.parquet(f'{embedded_dimensions_dir}{vintages_dir}/sales_{modality_name}').where(f.col('fiscal_week')<fw)
    #Pull basket information (sales, units, visits) for the given modality from the week we want to look at

    if modality_name == 'enterprise':
        df_modality_baskets = kpi.get_aggregate(
            start_date = start_date,
            end_date = end_date,
            metrics = ["sales","gr_visits","units"],
            join_with = 'stores', 
            apply_golden_rules = golden_rules(),
            group_by = ["ehhn","trn_dt","geo_div_no"],
        ).where(f.col('ehhn').isNotNull())
    else:
        #For this to run the latest UPC list for this week, we need to know the location of the latest segment's UPC list.  When a UPC list is created by the API, it writes a lookup file with the path to where the UPC list is.  This is a parameter passed in the API.  The logic below takes that path in the lookup file, pulls out segment, traverses up two levels, and then looks for the latest directory publish.  Then it takes the latest upc list of the segment to run the vintage.  All this looks like a good opportunity for a function
        upc_list_path_location = spark.read.format("delta").load(f'{embedded_dimensions_dir}{vintages_dir}/hh_{modality_name}/upc_list_path_lookup')
        upc_list_path_url = upc_list_path_location.select(upc_list_path_location.path)
        upc_list_path_url = upc_list_path_url.rdd.map(lambda column: column.path).collect()
        upc_list_path_url = " ".join(upc_list_path_url)
        segment = [Path(upc_list_path_url).parts[-1]]
        segment = " ".join(segment)
        upc_list_path_url_root = Path(upc_list_path_url).parents[1]
        upc_list_path_url_root = get_latest_modified_directory(upc_list_path_url_root.as_posix().replace('abfss:/', 'abfss://'))
        upc_list_path_url_newest = f'{upc_list_path_url_root}{segment}'
        upc_latest_location = spark.read.format("delta").load(upc_list_path_url_newest)
        
        df_modality_baskets = kpi.get_aggregate(
            start_date = start_date,
            end_date = end_date,
            metrics = ["sales","gr_visits","units"],
            join_with = 'stores', 
            apply_golden_rules = golden_rules(),
            group_by = ["ehhn","trn_dt","geo_div_no"],
            filter_by=Sifter(upc_latest_location, join_cond=Equality("gtin_no"), method="include")
        ).where(f.col('ehhn').isNotNull())
  
    #Aggregate by hh and fiscal week to get weekly sales, units, and visits data for the modality
    trans_agg = (df_modality_baskets.join(dates_tbl, 'trn_dt', 'inner')
                    .groupBy('ehhn', 'fiscal_week', 'fiscal_month', 'fiscal_quarter', 'fiscal_year')
                    .agg(f.sum('sales').alias('weekly_sales'),
                         f.sum('units').alias('weekly_units'),
                         f.sum('gr_visits').alias('weekly_visits')
                        )
                )

    #Find the minimum visit date in our new data pull
    ehhn_min_dt = df_modality_baskets.groupBy('ehhn').agg(f.min('trn_dt').alias('trn_dt'))
    #Find the division associated with each hh's first shop in our pull
    ehhn_div_min = df_modality_baskets.join(ehhn_min_dt, how = 'inner', on = ['ehhn','trn_dt'])

    #Get each household's vintage division (assuming this is their first shop in the modality, we check that later), along with the min fiscal week
    div_agg = (ehhn_div_min.join(dates_tbl, 'trn_dt', 'inner')
                         .orderBy('trn_dt')
                         .groupBy('ehhn')
                         .agg(f.first('geo_div_no').alias('vintage_div'),
                              f.min('fiscal_week').alias('fiscal_week')
                             )
            )
  
    #Find households that are in the new data who don't have a prior vintage, and get their vintage week (still called fiscal week at this stage)
    og_hh_vintage_ehhn = og_hh_vintage.select('ehhn')
    new_hhs = trans_agg.join(og_hh_vintage_ehhn, 'ehhn', 'leftanti').where(f.col('weekly_visits')>0)
    hhs_min_week = (new_hhs.groupBy('ehhn')
                          .agg(f.min('fiscal_week').alias('fiscal_week'))
                 )
  
    #Pull all vintage information for new hhs
    hh_vintage = (hhs_min_week.join(dates_tbl, 'fiscal_week', 'inner')
                            .join(div_agg,how = 'inner', on = ['ehhn','fiscal_week'])
                            .select('ehhn', 'fiscal_week', 'fiscal_year', 'fiscal_month', 'fiscal_quarter','vintage_div')
                            .distinct()
                            .withColumnRenamed('fiscal_week', 'vintage_week')
                            .withColumnRenamed('fiscal_year', 'vintage_year')
                            .withColumnRenamed('fiscal_month', 'vintage_period')
                            .withColumnRenamed('fiscal_quarter', 'vintage_quarter')
               )
    #Test to see if this is just one week
    hh_vintage.select('vintage_week').distinct().count()
    #Write out new week of vintage data
    hh_vintage.repartition(1).write.mode('overwrite').parquet(f'{embedded_dimensions_dir}{vintages_dir}/hh_{modality_name}/vintage_week={int(fw)}')
  
    #Read in what we just wrote to get to weekly transaction data
    new_hh_vintage = spark.read.parquet(f'{embedded_dimensions_dir}{vintages_dir}/hh_{modality_name}/vintage_week=*')
    #Make transaction level dataset 
    trans_agg_vintage = trans_agg.join(new_hh_vintage, 'ehhn', 'inner')
    #Write out vintage sales dataset
    trans_agg_vintage.repartition(1).write.mode('overwrite').parquet(f'{embedded_dimensions_dir}{vintages_dir}/sales_{modality_name}/fiscal_week={int(fw)}') 

    print(f'Modality {modality_name} completed for FW {fw}')
