# Databricks notebook source
# MAGIC %md
# MAGIC # Preliminary Investigation

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %pip install umap-learn scikit-learn==1.3

# COMMAND ----------

import numpy as np
import pandas as pd
import pickle as pkl
import sklearn as skl
from umap import UMAP
import plotly.express as px
import matplotlib.pyplot as plt

import pyspark.sql.functions as f

from effodata import ACDS, golden_rules

import sys
sys.path.append('..')

from resources import config
from products_embedding.src import utils

# COMMAND ----------

acds = ACDS(use_sample_mart=True)

# COMMAND ----------

cycle_date = '20231120'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Data

# COMMAND ----------

description_path = f'{config.product_vectors_description_path}/cycle_date={cycle_date}'
descriptions = spark.read.load(description_path)
descriptions.limit(20).display()

# COMMAND ----------

descriptions.count()

# COMMAND ----------

acds.products.limit(20).display()

# COMMAND ----------

[col for col in acds.products.columns if 'gtin' in col]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sample Query

# COMMAND ----------

search_query = 'jelly beans'

query_vector = utils.model.encode(search_query, normalize_embeddings=True).tolist()
query_vector

# COMMAND ----------

dot_df = utils.create_dot_df(descriptions, utils.create_array_query(query_vector))
dot_df.limit(20).display()

# COMMAND ----------

search_df = utils.create_search_df(dot_df)
search_df.limit(20).display()

# COMMAND ----------

search_df.join(acds.products, on='gtin_no', how='left').select('gtin_no', 'dot_product', 'dot_product_rank', 'bas_con_upc_no', 'bas_con_dsc_tx').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dimensionality Reduction

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.window import Window
from pyspark.sql.column import Column
import numpy as np
from umap import UMAP
import matplotlib.pyplot as plt
from pyspark import SparkContext
from sklearn.cluster import MiniBatchKMeans
from sklearn.cluster import KMeans
import pandas as pd
import sklearn
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, DoubleType

def make_umap(spark: SparkContext, 
              df = None, 
              prod_context = 'GTIN_NO', 
              n_neighbors=15, 
              min_dist=.5, 
              metric = 'euclidean', 
              n_components=2, 
              init='random', 
              random_state=0, 
              vector_list = None, 
              prod_list = None):
    """
    Returns a DataFrame with UMAP embeddings for a given list of vectors and products.

    Args:
        spark (SparkContext): The Spark context to use for creating DataFrames.
        df (DataFrame, optional): A DataFrame to join with the embeddings. If None, a new DataFrame is returned. Defaults to None.
        prod_context (str, optional): The name of the column in the DataFrame containing the product IDs. Defaults to 'GTIN_NO'.
        n_neighbors (int, optional): The number of neighbors to use for UMAP. Defaults to 15.
        min_dist (float, optional): The minimum distance between points in the UMAP embedding. Defaults to .5.
        metric (str, optional): The distance metric to use for UMAP. Defaults to 'euclidean'.
        n_components (int, optional): The number of dimensions in the UMAP embedding. Defaults to 2.
        init (str, optional): The initialization method for UMAP. Defaults to 'random'.
        random_state (int, optional): The random seed for UMAP. Defaults to 0.
        vector_list (list, optional): A list of vectors to embed. Either `vector_list` and `prod_list` or `df` must be provided. Defaults to None.
        prod_list (List[str], optional): A list of product IDs to associate with each vector. Either `vector_list` and `prod_list` or `df` must be provided. Defaults to None.

    Returns:
        DataFrame: A DataFrame with UMAP x,y cordinates for the given vectors and products.
    """
  
    umap_2d = UMAP(
        n_neighbors=n_neighbors,
        min_dist=min_dist, 
        metric=metric, 
        n_components=n_components, 
        init=init, 
        random_state=random_state
    )
    
    proj_2d = umap_2d.fit_transform(vector_list)
    embeddingsdf = pd.DataFrame()
    embeddingsdf['x'] = proj_2d[:,0]
    embeddingsdf['y'] = proj_2d[:,1]
    embeddingsdf[prod_context] = prod_list
    sparkDF=spark.createDataFrame(embeddingsdf) 
    if df: 
        return df.join(other=sparkDF, on=prod_context, how='inner')
    else:
        return sparkDF 

# COMMAND ----------

sample_upcs = descriptions#.limit(1000)
vector_list = [i[0] for i in sample_upcs.select('vector').collect()]
product_list = [i[0] for i in sample_upcs.select('gtin_no').collect()]

# COMMAND ----------

my_umap = make_umap(
    spark,
    vector_list=vector_list,
    prod_list=product_list
)

my_umap.display()

# COMMAND ----------

points = [(row.x, row.y) for row in my_umap.collect()]
points

# COMMAND ----------

import matplotlib.pyplot as plt



# COMMAND ----------

def create_scatter(points):
    plt.scatter([i[0] for i in points], [i[1] for i in points])

# COMMAND ----------

create_scatter([point for point in points
                if point[0] > 0 and point[0] < 1 and
                   point[1] > 0 and point[1] < 1])

# COMMAND ----------

my_umap2 = make_umap(
    spark,
    vector_list=vector_list,
    prod_list=product_list,
    n_neighbors=5, 
    min_dist=.5, 
)

points2 = [(row.x, row.y) for row in my_umap2.collect()]

create_scatter(points2)

# COMMAND ----------

def in_range(x, low, high):
    return x >= low and x <= high


def square_window(points, x_min, x_max, y_min, y_max):
    return [point for point in points
            if in_range(point[0], x_min, x_max) and
               in_range(point[1], y_min, y_max)]
    

def circle_window(points, x, y, r):
    return [point for point in points
            if (x - point[0])**2 + (y - point[1])**2 <= r**2]

# COMMAND ----------

create_scatter(square_window(points2, 0, 1, 0, 1))

# COMMAND ----------

create_scatter(circle_window(points2, 0, 0, 1))

# COMMAND ----------

my_umap3 = make_umap(
    spark,
    vector_list=vector_list,
    prod_list=product_list,
    n_neighbors=30, 
    min_dist=.5, 
)

points3 = [(row.x, row.y) for row in my_umap3.collect()]

create_scatter(points3)

# COMMAND ----------

create_scatter(square_window(points3, 0, 1, 0, 1))

# COMMAND ----------

create_scatter(circle_window(points3, 0, 0, .3))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Potential cluster metrics to track:
# MAGIC - number of clusters
# MAGIC - min/max/median/mean/stdev cluster size
# MAGIC - number of unclustered points (for DBSCAN)
# MAGIC - compare with basket colocation? (P(x in cart | y in cart))
# MAGIC
# MAGIC
# MAGIC Variables:
# MAGIC - embedding space (Steven's SBERT, concept2vec, query2concept2vec)
# MAGIC
# MAGIC
# MAGIC Pipeline:
# MAGIC Embedding Vectors -> Dimensionality Reduction (PCA, t-SNE, UMAP, None) -> Clustering Methodology (K-Means, DBSCAN, etc.) -> Cluster Evaluation

# COMMAND ----------

mat3 = np.array(points3)
mat3.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Clustering

# COMMAND ----------

# Attempt hdbscan clustering
hdb = sklearn.cluster.

# COMMAND ----------

sklearn.__version__

# COMMAND ----------

dir(sklearn.cluster)

# COMMAND ----------

np.save('/dbfs/FileStore/Users/p870220/non_endemic_umap.npy', mat3)

# COMMAND ----------

mat3 = np.load('/dbfs/FileStore/Users/p870220/non_endemic_umap.npy')
mat3.shape

# COMMAND ----------

hdb = skl.cluster.HDBSCAN()
hdb.fit(mat3)

with open('/dbfs/FileStore/Users/p870220/HDBSCAN.pkl', 'wb') as f:
    pkl.dump(hdb, f)

# COMMAND ----------

with open('/dbfs/FileStore/Users/p870220/HDBSCAN.pkl', 'rb') as f:
    hdb = pkl.load(f)

# COMMAND ----------

hdb

# COMMAND ----------

np.sum(hdb.labels_ == -1)

# COMMAND ----------

np.std

# COMMAND ----------

np.unique(hdb.labels_, return_counts=True)

# COMMAND ----------

def get_basic_cluster_stats(labels):
    # Transform data
    values, counts = np.unique(labels, return_counts=True)
    nonnull = counts[1:]

    # Basic stats
    num_samples = len(labels)
    num_unlabeled = counts[0]
    num_clusters = len(nonnull)
    avg_cluster_size = np.mean(nonnull)
    std_cluster_size = np.std(nonnull)
    min_cluster_size = np.min(nonnull)
    max_cluster_size = np.max(nonnull)

    out = {
        'num_samples': num_samples,
        'unlabeled_prop': num_unlabeled / num_samples,
        'num_clusters': num_clusters,
        'avg_cluster_size': avg_cluster_size,
        'std_cluster_size': std_cluster_size,
        'min_cluster_size': min_cluster_size,
        'max_cluster_size': max_cluster_size,
    }

    return out

get_basic_cluster_stats(hdb.labels_)

# COMMAND ----------

help(np.where)

# COMMAND ----------

size_cap = 200000
num_clusters = 10

x_vals = mat3[:size_cap, 0]
y_vals = mat3[:size_cap, 1]
relevant_labels = hdb.labels_[:size_cap]
color_code = np.where(relevant_labels == -1, -1, relevant_labels % (num_clusters - 1)).astype(str)

fig = px.scatter(
    x=x_vals,
    y=y_vals,
    color=color_code,
    hover_name=relevant_labels
)
fig.show()

# COMMAND ----------

mat3[hdb.labels_ == 756]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cluster Analysis

# COMMAND ----------

len(product_list)

# COMMAND ----------

# Construct DataFrame
gtin_groupings = spark.createDataFrame(pd.DataFrame({'gtin_no': product_list, 'label': hdb.labels_}))
gtin_groupings.limit(20).display()

# COMMAND ----------

gtin_groupings.groupBy('label').count().display()

# COMMAND ----------

# Join back onto ACDS.products
product_table = gtin_groupings.join(acds.products, on='gtin_no', how='inner')
product_table.count()

# COMMAND ----------

product_table.limit(20).display()

# COMMAND ----------

import pyspark.sql.functions as f

# Choose some random group and manually inspect the rows
product_table.filter(f.col('label') == 6).display()

# COMMAND ----------

(product_table
 .filter(f.col('label') >= 0)
 .groupBy('label')
 .agg(f.countDistinct('pid_fyt_pmy_dpt_cd').alias('num_primary_department'),
      f.countDistinct('pid_fyt_rec_dpt_cd').alias('num_recap_department'),
      f.countDistinct('pid_fyt_sub_dpt_cd').alias('num_sub_department'),
      f.countDistinct('pid_fyt_com_cd').alias('num_commodity'),
      f.countDistinct('pid_fyt_sub_com_cd').alias('num_sub_commodity'))
 .display())

# COMMAND ----------

product_table.filter(f.col('label') == 19769).display()

# COMMAND ----------



# COMMAND ----------

sklearn.metrics.silhouette_score(mat3, hdb.labels_)

# COMMAND ----------

sklearn.metrics.silhouette_score(vector_list, hdb.labels_)

# COMMAND ----------

hdb2 = skl.cluster.HDBSCAN(min_cluster_size=20)
hdb2.fit(mat3)

with open('/dbfs/FileStore/Users/p870220/HDBSCAN2.pkl', 'wb') as f:
    pkl.dump(hdb2, f)

# COMMAND ----------

with open('/dbfs/FileStore/Users/p870220/HDBSCAN2.pkl', 'rb') as f:
    hdb2 = pkl.load(f)

hdb2

# COMMAND ----------

get_basic_cluster_stats(hdb.labels_)

# COMMAND ----------

get_basic_cluster_stats(hdb2.labels_)

# COMMAND ----------


