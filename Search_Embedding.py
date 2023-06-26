# Databricks notebook source
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

# Set path for product vectors
product_vectors_path = get_latest_modified_directory("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/product_vectors_diet_description/") 
product_vectors_df = spark.read.format("delta").load(product_vectors_path)

# Specify the model directory on DBFS
model_dir = "/dbfs/dbfs/FileStore/users/s354840/pretrained_transformer_model" 

embedded_dimensions_dir = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions'
diet_query_embeddings_dir = embedded_dimensions_dir + '/diet_query_embeddings/cycle_date='
diet_upcs_dir = embedded_dimensions_dir + '/diet_upcs/cycle_date='
egress_dir = embedded_dimensions_dir + '/egress/upc_list'

# These packages are required for delivery
from sentence_transformers import SentenceTransformer, util

# Loading the transformer model
model = SentenceTransformer(model_dir) 

# get current datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta

# minus 1 year
today_year_ago = (datetime.today() - relativedelta(years=1)).strftime('%Y-%m-%d')
today = datetime.today().strftime('%Y-%m-%d')
today_time = datetime.now()
duration_time = (today_time + relativedelta(weeks=52))

# Get current ISO 8601 datetime in string format
iso_start_date = today_time.isoformat()
iso_end_date = duration_time.isoformat()

product_vectors_dir = '/product_vectors_diet_description/cycle_date='
diet_query_dir = '/diet_query_embeddings'

# Set path for product vectors
upc_list_path = get_latest_modified_directory(embedded_dimensions_dir + diet_query_dir) 
diet_query_embeddings_directories = spark.createDataFrame(list(dbutils.fs.ls(upc_list_path)))
diet_query_embeddings_directories_list = diet_query_embeddings_directories.rdd.map(lambda column: column.name).collect()

dbutils.widgets.text("embedding_query", "")
embedding_query = dbutils.widgets.get("embedding_query")
embedding_query=embedding_query.split(",")

#When we move forward with more of those below, we will need two different lists.  One for the query and one to create 
# 'FREE FROM GLUTEN', 'KETOGENIC', 'Lactose free', 'MACROBIOTIC', 'PALEO', 'VEGAN', 'VEGETARIAN', 'COELIAC', 'AYERVEDIC', 'LOW BACTERIA', 'DIABETIC', 
# ['Engine 2',  'GLYCEMIC', 'Grain free', 'HALAL', \
#'Healthy Eating', 'Healthy Pregnancy', 'Heart Friendly', 'HGC', 'High protein', 'KEHILLA', 'Kidney-Friendly', \
#'KOSHER', 'Low calorie', 'Low FODMAP', 'Low protein', 'Low salt', 'Mediterranean Diet', \
#'METABOLIC', 'NON_VEG', 'PECETARIAN', 'PLANT BASED', 'Plant Based Whole Foods Diet', 'RAW FOOD', \
#'VEG_OVO', 'WITHOUT BEEF', 'WITHOUT PORK']


# COMMAND ----------

#taking dot product of query vector and product vector
#order by dot product descending
from pyspark.sql import types as t
from pyspark.sql.column import Column
from pyspark.sql.window import Window
from pyspark.sql import functions as f
import numpy as np

@f.udf(returnType=t.FloatType())
def get_dot_product_udf(a: Column, b: Column):
    return float(np.asarray(a).dot(np.asarray(b)))

def create_upc_json(df, query):
  from pyspark.sql.functions import collect_list
  upc_list = df.rdd.map(lambda column: column.gtin_no).collect()
  upc_string = '","'.join(upc_list)
  
  upc_format = '{"cells":[{"order":0,"type":"BUYERS_OF_PRODUCT","purchasingPeriod":{"startDate":"'+ iso_start_date +'","endDate":"'+ iso_end_date +'","duration":52},"cellRefresh":"DYNAMIC","behaviorType":"BUYERS_OF_X","xProducts":{"upcs":["'+ upc_string +'"]},"purchasingModality":"ALL"}],"name":"'+ query +'","description":"Buyers of '+ query +' products."}'
  return upc_format

def create_search_df(dot_products_df):
  search_output_df = (dot_products_df.filter(f.col('dot_product')>=.3))
  return search_output_df

def create_dot_df(product_vectors_df, array_query_col):
  dot_products_df = (product_vectors_df.withColumn('dot_product', get_dot_product_udf("vector", array_query_col)).drop('vector').withColumn('dot_product_rank',f.rank().over(Window().orderBy(f.col('dot_product').desc()))))
  return dot_products_df

def create_array_query(query_vector):
  array_query_col = f.array([f.lit(i) for i in query_vector])
  return array_query_col

# COMMAND ----------


if embedding_query != '':
  embedding_queries = [embedding_query]
  for query in embedding_queries:
    query = query.lower()
    # Encoding the query, make it a pyspark vector we can take the dot product of later
    query_vector = model.encode(query, normalize_embeddings = True).tolist()
    search_df = create_search_df(create_dot_df(product_vectors_df, create_array_query(query_vector)))
    query = query.replace(' ', '_')
    query = query.replace('/', '')
    search_df.write.mode("overwrite").format("delta").save(upc_list_path + query)
    json_payload = create_upc_json(search_df, query)
    rdd = spark.sparkContext.parallelize([json_payload])
    df2 = spark.read.json(rdd)
    df2.coalesce(1).write.mode("overwrite").json(diet_upcs_dir + today + '/' + query)
    file_to_copy = get_latest_modified_directory(diet_upcs_dir + today + '/' + query)
    dbutils.fs.cp(file_to_copy, egress_dir +'/' + query + '_' + today + '.json')
else:
  embedding_queries = diet_query_embeddings_directories_list
  for query in embedding_queries:
    # Encoding the query, make it a pyspark vector we can take the dot product of later
    query_vector = model.encode(query, normalize_embeddings = True).tolist()
    search_df = create_search_df(create_dot_df(product_vectors_df, create_array_query(query_vector)))
    query = query.replace(' ', '_')
    query = query.replace('/', '')
    search_df.write.mode("overwrite").format("delta").save(diet_query_embeddings_dir + today + '/' + query)
    json_payload = create_upc_json(search_df, query)
    rdd = spark.sparkContext.parallelize([json_payload])
    df2 = spark.read.json(rdd)
    df2.coalesce(1).write.mode("overwrite").json(diet_upcs_dir + today + '/' + query)
    file_to_copy = get_latest_modified_directory(diet_upcs_dir + today + '/' + query)
    dbutils.fs.cp(file_to_copy, egress_dir +'/' + query + '_' + today + '.json')

# COMMAND ----------

dbutils.notebook.exit("Search_Embedding completed")
