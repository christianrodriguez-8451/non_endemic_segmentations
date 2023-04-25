# Databricks notebook source
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
product_vectors_path = get_latest_modified_directory("abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/Users/s354840/embedded_dimensions/allpimto_product_vectors_diet_description/") 
product_vectors_path = product_vectors_path + 'upc_vectors/'
product_vectors_df = spark.read.parquet(product_vectors_path)

# Specify the model directory on DBFS
model_dir = "/dbfs/dbfs/FileStore/users/s354840/pretrained_transformer_model" 

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

embedding_queries = ['paleo', 'vegan', 'ketogenic']

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

def create_upc_json(df):
  from pyspark.sql.functions import collect_list
  upc_list = df.rdd.map(lambda column: column.gtin_no).collect()
  upc_string = '","'.join(upc_list)
  
  upc_format = '{"cells":[{"order":0,"type":"BUYERS_OF_PRODUCT","purchasingPeriod":{"startDate":"'+ iso_start_date +'","endDate":"'+ iso_end_date +'","duration":52},"cellRefresh":"DYNAMIC","behaviorType":"BUYERS_OF_X","xProducts":{"upcs":["'+ upc_string +'"]},"purchasingModality":"ALL"}],"name":"Stevens Brand New Segment","description":"A description here, but its totally optional."}'
  return upc_format

def create_search_df(df):
  search_output_df = (dot_products_df.filter(f.col('dot_product')>=.3))
  return search_output_df

def create_dot_df(product_vectors_df, array_query_col):
  dot_products_df = (product_vectors_df.withColumn('dot_product', get_dot_product_udf("vector", array_query_col)).drop('vector').withColumn('dot_product_rank',f.rank().over(Window().orderBy(f.col('dot_product').desc()))))
  return dot_products_df

def create_array_query(query_vector):
  array_query_col = f.array([f.lit(i) for i in query_vector])
  return array_query_col

# COMMAND ----------

for query in embedding_queries:
  # Encoding the query, make it a pyspark vector we can take the dot product of later
  query_vector = model.encode(query, normalize_embeddings = True).tolist()
  json_payload = create_upc_json(create_search_df(create_dot_df(product_vectors_df, create_array_query(query_vector))))
  rdd = spark.sparkContext.parallelize(json_payload)
  df2 = spark.read.json(rdd)
  df2.write.mode("overwrite").json('abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/Users/s354840/embedded_dimensions/diet_upcs/cycle_date=' + today + '/diet_' + query + '_' + today + '.json')
