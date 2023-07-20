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

product_path = "abfss://acds@sa8451posprd.dfs.core.windows.net/product/current"
product_dim = spark.read.parquet(product_path)
product_dim = product_dim.select('con_upc_no','pid_fyt_rec_dpt_dsc_tx','pid_fyt_com_dsc_tx','con_dsc_tx')

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

def display_upcs(x):
  ldf=pd.DataFrame(x,columns=['UPC'])
  ldf['IMG']=ldf['UPC'].map(lambda x: f'<img src="https://www.kroger.com/product/images/medium/front/{x}">' )#if x.isnumeric() else '')
  return ldf.T.style

manager = SparkManager()

acds = ACDS(use_sample_mart=False)
kpi = KPI(use_sample_mart=False)

# get current datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta

# minus 1 year
today_year_ago = (datetime.today() - relativedelta(years=1)).strftime('%Y%m%d')
today = datetime.today().strftime('%Y%m%d')

# Look at acds transactions for all households the past year
acds_previous_year_transactions = acds.get_transactions(today_year_ago, today, apply_golden_rules=golden_rules(['customer_exclusions', "fuel_exclusions"]))
acds_previous_year_transactions = acds_previous_year_transactions.where(acds_previous_year_transactions.scn_unt_qy > 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diet and description based product attribute embeddings.

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws

for i in list_of_dirs:
  vectors_path = upc_list_path + i
  vectors_path = spark.read.parquet(vectors_path)
  
  upcs_prod = vectors_path.join(product_dim, vectors_path.gtin_no == product_dim.con_upc_no, 'inner')
  
  query = i
  query = query.replace('diet_', '')
  query = query.replace('/', '')
  condition = 'diets'
  condition2 = 'diets.' + query #+ '.isNotNull()'

  pinto_upc = pinto_prods.select("upcs.standard","name").where(array_contains(pinto_prods.dietList.slug, query))
  pinto_upc_df = pinto_upc.withColumn("standard", concat_ws(",",col("standard")))
  pinto_upc_df = pinto_upc_df.withColumn("trim_standard", pinto_upc_df.standard.substr(1,13))
  join_pinto_upc = upcs_prod.join(pinto_upc_df, upcs_prod.con_upc_no == pinto_upc_df.trim_standard, 'outer')
  pim_upcs = pim_core.select(pim_core.upc_key, pim_core.krogerOwnedEcommerceDescription, condition)#.where(condition)
  pim_upcs = pim_upcs.filter(col(condition2).isNotNull())
  pim_join_pinto_upc = join_pinto_upc.join(pim_upcs, pim_upcs.upc_key == join_pinto_upc.gtin_no, 'outer')
  upcs = pim_join_pinto_upc.rdd.map(lambda column: column.gtin_no).collect()


  display_upcs(upcs)
  print(query + ' product(gtin_no) search results ranked(dot_rduct_rank) by UPC similarity score(dot_product) to search query.  Columns standard, name, trim_standard represent if Pinto defined the product as ' + query + '.  Columns upc_key, krogerOwnedEcommerceDescription represent if PIM defined the product as ' + query + '.  Columns con_upc_no, pid_fyt_rec_dpt_dsc_tx, con_dsc_tx are from ACDS product and represent items that were not defined by Pinto or PIM as ' + query + ' but the product vector is similar to a ' + query + ' defined product.')
  pim_join_pinto_upc.orderBy(pim_join_pinto_upc.dot_product.desc()).display()
  
  #print('Distribution of ' + query + ' search results across recap department by product count.')
  upcs_prod.select('pid_fyt_rec_dpt_dsc_tx','con_dsc_tx').groupBy('pid_fyt_rec_dpt_dsc_tx').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What do the transactions look for the top 1% highest similarity scored UPCs

# COMMAND ----------

vectors_path = upc_list_path + 'ketogenic'
vectors_path = spark.read.parquet(vectors_path)

from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank, col

window = Window.partitionBy().orderBy(vectors_path['dot_product'].desc())

top_percent_keto_upcs = vectors_path.select('*', percent_rank().over(window).alias('rank')).filter(col('rank') <= 0.05) # top % for example 
keto_upcs = top_percent_keto_upcs.rdd.map(lambda column: column.gtin_no).collect()

import pandas as pd
def display_upcs(x):
  ldf=pd.DataFrame(x,columns=['UPC'])
  ldf['IMG']=ldf['UPC'].map(lambda x: f'<img src="https://www.kroger.com/product/images/medium/front/{x}">' )#if x.isnumeric() else '')
  return ldf.T.style

display_upcs(keto_upcs)

# COMMAND ----------

top_percent_keto_aggregates = kpi.get_aggregate(
    start_date=today_year_ago,
    end_date=today,
    group_by=['gtin_no'],
    metrics=['sales', 'units', 'visits', 'households', 'stores_selling'],
    filter_by=Sifter(top_percent_keto_upcs, join_cond=Equality("gtin_no"), method="include")
)
top_2_aggregates = top_percent_keto_aggregates.join(top_percent_keto_upcs, top_percent_keto_upcs.gtin_no == top_percent_keto_aggregates.gtin_no).drop(top_percent_keto_aggregates.gtin_no)
top_2_aggregates.display()
top_2_aggregates.agg({'households':'sum'}).display()

# COMMAND ----------

top_2_aggregates.agg({'sales': 'avg', 'units': 'avg', 'stores_selling':'avg', 'households':'avg'}).display()

# COMMAND ----------

top_2_aggregates.where((top_2_aggregates.stores_selling > '1950')).display()

# COMMAND ----------

store_pen_over_average = top_2_aggregates.where((top_2_aggregates.stores_selling > '1950'))
store_pen_over_average_upcs = store_pen_over_average.select(store_pen_over_average.gtin_no)
store_pen_over_average_upcs = store_pen_over_average_upcs.rdd.map(lambda column: column.gtin_no).collect()
display_upcs(store_pen_over_average_upcs)

# COMMAND ----------

top_percent_upc_transactions = acds_previous_year_transactions.join(store_pen_over_average, store_pen_over_average.gtin_no == acds_previous_year_transactions.gtin_no)


#product 100% keto product, create that layer.  x and y
#sales distribution, count of hh, and low sales
#is there a way come up with a combined score, someone is buying 5 out of 6 categories.  the more in that category

# COMMAND ----------

top_percent_upc_transactions.select(top_percent_upc_transactions.ehhn).distinct().count()#20% 44328608hh 20%with>918stores sold43495694 20%w/1950stores 41119990

# COMMAND ----------

from pyspark.sql.functions import max
store_pen_70_percent = top_2_aggregates.select(max(top_2_aggregates.stores_selling)*.7)

# COMMAND ----------

top_2_aggregates.where(top_2_aggregates.stores_selling > store_pen_70_percent).display()

# COMMAND ----------

store_penetration_70_percent = store_pen_70_percent.display()

# COMMAND ----------

kpi.get_aggregate(
    start_date=today_year_ago,
    end_date=today,
    group_by=['gtin_no'],
    metrics=['sales', 'units', 'visits', 'households', 'stores_selling'],
    filter_by=Sifter(pim_join_pinto_upc, join_cond=Equality("gtin_no"), method="include"),
    query_filters="gtin_no in ('0978059323340', '0085001466903', '0084223400074', '0084223400128', '0084223400130', '0063803170570', '0085003210942', '0084223400129', '0076733501117', '0004223400059')"
).toPandas()

# COMMAND ----------

 pinto_scores_uc_diet_reshaped = pinto_scores_uc_dietList.groupby(['upc']).pivot(pinto_attribute).agg(count(lit(1))).na.fill(0) \
        .select('upc', pinto_name) \
        .withColumn('upc', lpad('upc', 13, '0'))

# COMMAND ----------

#ketogenic counts
hml_weighted_df.groupBy(hml_weighted_df.spend_rank, hml_weighted_df.penetration_rank).count().display()

# COMMAND ----------

hml_weighted_df.groupBy(hml_weighted_df.spend_rank, hml_weighted_df.penetration_rank).count().display()

# COMMAND ----------

final_segs = (spark.read.format("delta").load(segment_filepath)
                  .filter(f.col('modality') == 'ketogenic')
                  .filter(f.col('stratum_week') == stratum_week)
             )what_weights_looklike = spark.read.format("delta").load(weights_filepath + '/modality=ketogenic/stratum_week=20230501')
hml_weighted_df.display()
final_segs.join(hml_weighted_df, final_segs.ehhn == hml_weighted_df.ehhn).where(final_segs.segment == 'H').display()
final_segs.groupBy(final_segs.segment).count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Household behavior

# COMMAND ----------

vegan_segmentation_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/segment_behavior/segmentation/modality=vegan/stratum_week=20230503/" 
vegan_segmentation = spark.read.format("delta").load(vegan_segmentation_path)

# COMMAND ----------

vegan_segmentation.groupBy(vegan_segmentation.segment).count().display()

# COMMAND ----------


