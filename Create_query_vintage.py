# Databricks notebook source
spark.conf.set("spark.sql.shuffle.partitions","auto")

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

# Set path for product vectors
upc_list_path = get_latest_modified_directory(embedded_dimensions_dir + diet_query_dir) 

#can loop through vector list here
upc_vectors_path = upc_list_path + 'vegan'
upc_vectors_dot_product = spark.read.format("delta").load(upc_vectors_path)

upc_vectors_dot_product.count()

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

#pickup and delivery and instore updated through Jan 1 
fiscal_st_dt = '20210101'
fiscal_end_dt = '20231231' 
modality_acds = "vegan" #only parameter that will change from INSTORE,PICKUP or DELIVERY
#filenaming 
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

new_hh_vintage = spark.read.parquet(embedded_dimensions_dir + vintages_dir + '/hh_' + modality_name)


# COMMAND ----------

trans_agg_vintage = trans_agg.join(new_hh_vintage, 'ehhn', 'inner')

trans_agg_vintage.coalesce(1).write.mode('overwrite').partitionBy('fiscal_week').parquet(embedded_dimensions_dir + vintages_dir + '/sales_' + modality_name)

new_hh_vintage.count()

display(new_hh_vintage.groupBy('vintage_week').agg(f.countDistinct('ehhn').alias('hhs')))
#df_ecomm_baskets.display()
#pickup_bsk.display()
#trans_agg.display()
#pickup_bsk_div.display()
#ehhn_min_dt.display()
#ehhn_div_min.display()
#div_agg.display()
#new_hhs.display()
#hhs_min_week.display()
#hh_vintage.display()
#new_hh_vintage.display()

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

#Parameters, only thing to change is modality list if you want to run fewer modalities
modality_list_nonship = ['delivery', 'pickup', 'instore', 'ocado_pickup_all', 'ocado_delivery_all', 'ocado_fc003', 'fc_tampa_vs', 'fc_jacksonville_vs', 'fc_miami_vs', 'fc_orlando_vs', 'fc_austin_vs', 'fc_birmingham_vs','fc_okcity_vs','fc_sanantonio_vs','ocado_fc004','ocado_fc001','ocado_fc005','ocado_fc010','ocado_fc006','ocado_fc007','fc_indy_vs','fc_lville_vs','fc_columbus_vs','fc_nashville_vs','fc_chicago_vs','fc_cincy_vs','fc_atlanta_vs','fc_pprairie_vs','fc_dallas_vs','fc_norky_vs','fc_fortcol_vs']
#['paleo', 'vegan', 'ketogenic']
fw = get_fw(1)
start_date = get_start_date(1)
end_date = get_end_date(1)

modality_list_HT = ['ocado_fc002']

# COMMAND ----------

for modality_name in modality_list_nonship:
    trans_max_date = acds.get_transactions(start_date = start_date,
                                 end_date = end_date,
                                 apply_golden_rules = modality_config[modality_name]["golden_rules"],
                                 query_filters = modality_config[modality_name]["query_filters"]).agg(f.max('trn_dt').alias('max_date')).collect()[0]['max_date']
    print(f'Modality {modality_name} has data through {trans_max_date} for FW {fw}')

    assert trans_max_date == int(end_date), f"Error: Data for FW {fw} is not fully updated for modality {modality_name}. We need data through {end_date} but it is only in ACDS through {trans_max_date}."
  

# COMMAND ----------


        'combo_modality_dict_nonship':{'delivery_with_ocado':['delivery', 'ocado_delivery_all'], 
                                       'pickup_and_delivery':['pickup', 'delivery'], 
                                       'pickup_with_ocado':['pickup', 'ocado_pickup_all'], 
                                       'pickup_and_delivery_with_ocado':['pickup', 'delivery', 'ocado_pickup_all', 'ocado_delivery_all'],
                                       'ocado_new_markets':['ocado_fc003','fc_austin_vs','fc_birmingham_vs','fc_okcity_vs','fc_sanantonio_vs'],
                                       'enterprise_nonship':['instore', 'pickup', 'delivery', 'ocado_pickup_all', 'ocado_delivery_all']
                                  },

# COMMAND ----------

for modality in modality_dict_nonship:
    #Pull existing vintage hh and sales data for the combo modality
    og_hh_vintage = spark.read.parquet(f'{config["storage"]["vintages_folder"]}/combinations/hh_{modality}').where(f.col('vintage_week')<fw)
    og_sales = spark.read.parquet(f'{config["storage"]["vintages_folder"]}/combinations/sales_{modality}').where(f.col('fiscal_week')<fw)

    #Pull newest week of vintage HH and sales data for individual modalities that make up the combo modality
    vintage_dfs = []
    sales_dfs = []
    for individual_modality in modality_dict_nonship[modality]:
        vintage_df = spark.read.parquet(f'{config["storage"]["vintages_folder"]}/hh_{individual_modality}/vintage_week={fw}')
        sales_df = spark.read.parquet(f'{config["storage"]["vintages_folder"]}/sales_{individual_modality}/fiscal_week={fw}')
        vintage_dfs.append(vintage_df)
        sales_dfs.append(sales_df)

    #Combine newest week of vintage HH and sales data into one long df each (may be multiple of the same EHHN in dataset at this point)
    newest_week_hh_vintage = reduce(DataFrame.unionByName, vintage_dfs)
    newest_week_sales = reduce(DataFrame.unionByName, sales_dfs)

    #Group so each newest week df has one row of data per ehhn
    #1 small nuance is we arbitarily take the minimum division number if a HH happens to shop 2 divisions in the same week (feels really unlikely) for their first modality shop. 
    newest_week_hh_vintage = (newest_week_hh_vintage
                              .groupBy('ehhn')
                              .agg(f.min('vintage_year').alias('vintage_year'), 
                                   f.min('vintage_period').alias('vintage_period'), 
                                   f.min('vintage_quarter').alias('vintage_quarter'), 
                                   f.min('vintage_week').alias('vintage_week'), 
                                   f.min('vintage_div').alias('vintage_div')
                                  )
                           )
  
    newest_week_sales = (newest_week_sales
                         .groupBy('ehhn','fiscal_week','fiscal_month','fiscal_quarter','fiscal_year')
                         .agg(f.sum('weekly_sales').alias('weekly_sales'), 
                              f.sum('weekly_units').alias('weekly_units'),
                              f.sum('weekly_visits').alias('weekly_visits')
                             )
                      )
  
    #Anti join to existing vintage so that only new hhs are added, then write out.
    newest_week_hh_vintage_new_hhs_only = newest_week_hh_vintage.join(og_hh_vintage, on = 'ehhn', how = 'leftanti')
    newest_week_hh_vintage_new_hhs_only.repartition(1).write.mode('overwrite').parquet(f'{config["storage"]["vintages_folder"]}/combinations/hh_{modality}/vintage_week={int(fw)}')

    #Now pull all vintage info
    hh_vintage = spark.read.parquet(f'{config["storage"]["vintages_folder"]}/combinations/hh_{modality}').where(f.col('vintage_week')<=fw)

    #Now join vintage info and newest week sales and write out sales data for this fiscal week
    newest_week_sales_fw = newest_week_sales.join(hh_vintage, on = 'ehhn', how = 'inner')
    newest_week_sales_fw.repartition(1).write.mode('overwrite').parquet(f'{config["storage"]["vintages_folder"]}/combinations/sales_{modality}/fiscal_week={int(fw)}')
    print(f"Modality {modality} finished")

# COMMAND ----------


