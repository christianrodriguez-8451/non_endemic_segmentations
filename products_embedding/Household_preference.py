# Databricks notebook source
# MAGIC %pip install poirot
# MAGIC %pip install effodata
# MAGIC %pip install kpi_metrics

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
storage_account = 'sa8451entlakegrnprd'

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

#Define service principals
service_credential = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-pw')
service_application_id = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-app-id')
directory_id = "5f9dc6bd-f38a-454a-864c-c803691193c5"
storage_account = 'sa8451camprd'

# COMMAND ----------

#Set configurations
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

mda = spark.read.parquet('abfss://measure@sa8451camprd.dfs.core.windows.net/dashboard/campaign/version=v2')

# COMMAND ----------

mda.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define product data paths to join with to understand products being returned by query embeddings.  

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

from pyspark.sql.functions import array_contains
import pyspark.sql.functions as f 

product_path = "abfss://acds@sa8451posprd.dfs.core.windows.net/product/current"
product_dim = spark.read.parquet(product_path)
product_dim = product_dim.select('con_upc_no','pid_fyt_rec_dpt_dsc_tx','con_dsc_tx')

pinto_path = get_latest_modified_directory('abfss://data@sa8451entlakegrnprd.dfs.core.windows.net/source/third_party/prd/pinto/')
pinto_prods = spark.read.parquet(pinto_path)
pim_path = get_latest_modified_directory("abfss://pim@sa8451posprd.dfs.core.windows.net/pim_core/by_cycle/")
pim_core = spark.read.parquet(pim_path)
# Set path for product vectors
upc_list_path = get_latest_modified_directory("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/Users/s354840/embedded_dimensions/diet_query_embeddings/") 

latest_diet_embeddings = list(dbutils.fs.ls(upc_list_path))
df = spark.createDataFrame(latest_diet_embeddings,['FullFilePath', 'directory'])
list_of_dirs_df = df.select(df.directory)

from pyspark.sql.functions import collect_set, substring_index, concat_ws, concat, split, regexp_replace, size, expr
#list_of_embeddings = list_of_dirs_df.withColumn("trim",expr("substring(directory, 1, length(directory)-1)"))

from pyspark.sql.functions import collect_list
#list_of_embeddings = list_of_embeddings.rdd.map(lambda column: column.trim).collect()
list_of_dirs = list_of_dirs_df.rdd.map(lambda column: column.directory).collect()

# IMPORT PACKAGES
import pandas as pd
pd.set_option('display.max_columns',500)
from poirot import SparkManager
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

manager = SparkManager()

acds = ACDS(use_sample_mart=False)
kpi = KPI(use_sample_mart=False)

# get current datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta

# minus 1 year
today_year_ago = (datetime.today() - relativedelta(years=1)).strftime('%Y%m%d')
today = datetime.today().strftime('%Y%m%d')

# COMMAND ----------

# Look at acds transactions for all households the past year
acds_previous_year_transactions = acds.get_transactions(today_year_ago, today, apply_golden_rules=golden_rules(['customer_exclusions', "fuel_exclusions"]))
acds_previous_year_transactions = acds_previous_year_transactions.where(acds_previous_year_transactions.scn_unt_qy > 0)

#can loop through vector list here
vectors_path = upc_list_path + 'ketogenic'
vectors_path = spark.read.parquet(vectors_path)

from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank, col
window = Window.partitionBy().orderBy(vectors_path['dot_product'].desc())

vectors_sold_last_year = vectors_path.join(acds_previous_year_transactions, vectors_path.gtin_no == acds_previous_year_transactions.gtin_no, 'inner')
acds_previous_year_transactions_with_vectors = vectors_path.join(acds_previous_year_transactions, vectors_path.gtin_no == acds_previous_year_transactions.gtin_no, 'outer')
#acds_previous_year_total_units_by_hh = acds_previous_year_transactions.groupBy(f.col('ehhn')).agg(f.sum(f.col('scn_unt_qy')).alias('Total Units'))

# COMMAND ----------

acds_previous_year_transactions_with_vectors = acds_previous_year_transactions_with_vectors.where(acds_previous_year_transactions_with_vectors.scn_unt_qy > 0)
acds_previous_year_transactions.where(acds_previous_year_transactions.ehhn == '103852348').display()

# COMMAND ----------

#distinct_vector_hh = vectors_sold_last_year.select(vectors_sold_last_year.ehhn).distinct() # count()55,798,318
#Replace 0 for null on only population column 
zero_null_dot_product = acds_previous_year_transactions_with_vectors.na.fill(value=0,subset=["dot_product"]).distinct()

preference_units_by_hh = zero_null_dot_product.groupBy(f.col('ehhn'), f.col('transactions.gtin_no')).agg((f.sum(f.col('scn_unt_qy')*(f.col('dot_product')))).alias('preference_units'))
#preference_units_by_hh.where(preference_units_by_hh.preference_units.isNotNull()).display()
#141933215
#0001111001063
#0.44375500082969666
#141933215
#0001111060919
#0.47875481843948364
#zero_null_dot_product.where((f.col('transactions.gtin_no') == '0020324300000') & (f.col('transactions.ehhn') == '103852348')).display()

# COMMAND ----------

#preference_units_by_hh.where(preference_units_by_hh.ehhn == '103852348').display()

# COMMAND ----------

preference_units_by_hh_totals = preference_units_by_hh.groupBy(f.col('ehhn')).agg(f.sum(f.col('preference_units')).alias('total_preference_units'))

# COMMAND ----------

#preference_units_by_hh_totals.where(preference_units_by_hh_totals.total_preference_units.isNotNull()).display()

# COMMAND ----------

#group by HH sum of units for vector purchase
acds_previous_year_total_units_by_hh = acds_previous_year_transactions.groupBy(f.col('ehhn')).agg(f.sum(f.col('scn_unt_qy')).alias('Total_Units'))
#vector_total_units_by_hh = vectors_sold_last_year.groupBy(f.col('ehhn')).agg(f.sum(f.col('scn_unt_qy')).alias('Total_Units'))
#vector_total_units_by_hh.display()

# COMMAND ----------

units_join = preference_units_by_hh_totals.join(acds_previous_year_total_units_by_hh, acds_previous_year_total_units_by_hh.ehhn == preference_units_by_hh_totals.ehhn)
NormalizedPreference = units_join.withColumn("NormalizedPreference", (f.col("total_preference_units") / f.col("Total_Units")))
NormalizedPreference = NormalizedPreference.where(NormalizedPreference.NormalizedPreference != 0) 
NormalizedPreference.orderBy(NormalizedPreference.NormalizedPreference.desc()).display()

# COMMAND ----------

#NormalizedPreference.where(NormalizedPreference.NormalizedPreference > 1).display()
NormalizedPreference.write.mode("overwrite").parquet('abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/Users/s354840/embedded_dimensions/allpimto_product_vectors_diet_description/cycle_date=' + today + '/NormalizedPreference')

# COMMAND ----------

    hh_health_score = rescale_and_round(NormalizedPreference, 'NormalizedPreference', 'NormalizedPreference_score_rescaled', 0, 100, NormalizedPreference.agg({'NormalizedPreference':'min'}).collect()[0][0], NormalizedPreference.agg({'NormalizedPreference':'max'}).collect()[0][0])

# COMMAND ----------

NormalizedPreference.where(NormalizedPreference.NormalizedPreference > 10).count()#total=62548606#total>0=55711393
#top_20_percent_hhs.count()#62548606#55711393

# COMMAND ----------

vectors_total_units = kpi.get_aggregate(
    start_date='20220102',
    end_date='20220212',
    metrics = ["sales","gr_visits","units"],
    join_with = 'stores', #need this to use geo
    apply_golden_rules = golden_rules(), #will need to remove store exclusions GR for Ocado and Ship (Vitacost as Division). this by default will include ["011","014","016","018","021","024","025","026","029","034","035","105","531","534","615","620","660","701","703","705","706"] per https://github.8451.com/FoundationalComponents/GoldenRules/blob/master/Store_Exclusions.md
    group_by = ["ehhn","trn_dt","geo_div_no"],
    query_filters=[
        "ehhn == '99898545' "]
)


# COMMAND ----------

vectors_total_units.display()

# COMMAND ----------

dane_renata_total_units = kpi.get_aggregate(
    start_date='20230101',
    end_date='20231231',
    metrics = ['last_sold_date',
 'sales',
 'gross_sales',
 'units',
 'visits',
 'gr_visits',
 'sales_per_visit',
 'units_per_visit',
 'sales_per_unit',
 'product_count',
 'stores_selling',
 'sales_per_store',
 'units_per_store',
 'kroger_coupon_discount_sales',
 'kroger_coupon_discount_units',
 'kroger_match_coupon_discount_sales',
 'kroger_match_coupon_discount_units',
 'vendor_discount_sales',
 'vendor_discount_units',
 'retailer_loyalty_discount_sales',
 'retailer_loyalty_discount_units',
 'total_discount_sales',
 'total_discount_units'],
    query_filters=[
        "ehhn == '10933600025' "]
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Dane and Renata's Kroger sales for 2022 and contribution to Steven's bonus :)

# COMMAND ----------

dane_renata_total_units.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dane and Renata's Kroger sales for 2023 and contribution to Steven's bonus :)

# COMMAND ----------

dane_renata_total_units.display()

# COMMAND ----------

last_week_keto = spark.read.parquet('abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/vintages/sales_ketogenic/fiscal_week=20230404')
last_week_keto.display()

# COMMAND ----------

txns_daily = acds.get_transactions(start_date='20230518', end_date="20230525")


# COMMAND ----------

txns_340764942 = txns_daily.where(txns_daily.ehhn == '340764942')
txns_340764942.display()

# COMMAND ----------

txns_340764942.join(vectors_path, txns_340764942.gtin_no == vectors_path.gtin_no).display()

# COMMAND ----------


