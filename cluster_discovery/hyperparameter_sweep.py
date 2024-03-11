# Databricks notebook source
# MAGIC %md
# MAGIC # Clustering Hyperparameter Sweep
# MAGIC
# MAGIC This notebook is designed to run over a long break. It will construct clusterings for different combinations of dimensionality reduction and clustering techniques. Dimensionality reduction techniques include:
# MAGIC
# MAGIC - none
# MAGIC - PCA
# MAGIC - t-SNE
# MAGIC - UMAP
# MAGIC
# MAGIC Clustering techniques include:
# MAGIC
# MAGIC - k-means
# MAGIC - HDBSCAN
# MAGIC
# MAGIC After all clusters have been computed, we derive some relevant benchmarks for each clustering, including:
# MAGIC
# MAGIC - Cluster size statistics (min, q1, median, q3, max, mean, stdev)
# MAGIC - elbow plot, silhouette, gap statistic, dendrogram, Davies-Bouldin Index, Calinski-Harabasz Index
# MAGIC   - both on reduced and full data
# MAGIC - some kind of product group entropy/information? maybe at different product hierarchy levels
# MAGIC
# MAGIC Helpful Resources:
# MAGIC
# MAGIC - [Cluster Evaluation](https://www.geeksforgeeks.org/dunn-index-and-db-index-cluster-validity-indices-set-1/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

# Python imports
import json
import numpy as np
import pickle as pkl
from sklearn.manifold import trustworthiness

# Company imports
from effodata import ACDS, golden_rules

# Local path workaround
import sys
sys.path.append('..')

# Local imports
from resources import config as config_nonendemic
from lib import config, dispatching, utils

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

parts_to_run = {
    'data_loading',

    # Dimensionality Reduction
    'baseline',
    'pca',
    'tSNE',
    'UMAP',

    # Clustering
    'kmeans',
    'HDBSCAN',
    'OPTICS',

    'evaluation'
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

# Load UPCs from non-endemic pipeline
if 'data_loading' in parts_to_run:
    description_path = f'{config_nonendemic.product_vectors_description_path}/cycle_date={config.EMBEDDING_DATA_WEEK}'
    descriptions_df = spark.read.load(description_path)
    descriptions_df.limit(20).display()

# COMMAND ----------

# Collect for local storage
if 'data_loading' in parts_to_run:
    collected = descriptions_df.collect()
    gtin_nos = [row.gtin_no for row in collected]#[:config.UNIVERSE_SIZE]
    vectors = np.array([row.vector for row in collected])#[:config.UNIVERSE_SIZE]

    print(gtin_nos[:20])
    print(f'{len(gtin_nos)} total UPCs')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

descriptions_df.select('gtin_no').distinct().count()

# COMMAND ----------

descriptions_df.distinct().count()

# COMMAND ----------

descriptions_df.groupBy('gtin_no', 'vector').count().orderBy('count', ascending=False).display()

# COMMAND ----------

full_duplicates = descriptions_df.groupBy('gtin_no', 'vector').count().filter(f.col('count') > 1).select('gtin_no')
partial_duplicates = descriptions_df.groupBy('gtin_no').count().filter(f.col('count') > 1).select('gtin_no')
partial_duplicates.join(full_duplicates, on='gtin_no', how='anti').display()

# COMMAND ----------

full_duplicates.count(), partial_duplicates.count()

# COMMAND ----------

descriptions_df.groupBy('gtin_no').count().orderBy('count', ascending=False).display()

# COMMAND ----------

import pyspark.sql.functions as f
descriptions_df.filter(f.col('gtin_no').isNotNull()).count()

# COMMAND ----------

descriptions_df.filter(f.col('gtin_no') == '0001111001619').display()

# COMMAND ----------

descriptions_df.filter(f.col('gtin_no') == '0019545028727').distinct().display()

# COMMAND ----------

len(gtin_nos), len(set(gtin_nos))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Write out
if 'data_loading' in parts_to_run:
    with open(config.upc_path, 'wb') as fo:
        pkl.dump(gtin_nos, fo)

    np.save(config.embedding_path, vectors)

# COMMAND ----------

vectors = np.load(config.embedding_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Dimensionality Reduction
# MAGIC
# MAGIC Output Format:
# MAGIC
# MAGIC - unique_id
# MAGIC - algorithm_type
# MAGIC - hparam_dict
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Baseline

# COMMAND ----------

if 'baseline' in parts_to_run:
    np.save(f'{config.spark_to_file(config.artifact_paths["dimensionality"])}/identity.npy', vectors)

    # Create table
    dr_log_data = [('baseline', 'identity', json.dumps({}), 1.0)]

    dr_log = spark.createDataFrame(dr_log_data, schema=config.dr_log_schema)
    dr_log.write.mode('overwrite').format('delta').save(config.log_paths['dimensionality'])
    dr_log.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PCA

# COMMAND ----------

approach_name = 'pca'

# COMMAND ----------

# List the parameters
param_configs = utils.grid_search(config.dimensionality_reduction_hyperparameters[approach_name])
param_configs

# COMMAND ----------

# Execute
if approach_name in parts_to_run:
    notebooks = [
        dispatching.NotebookData(
            config.notebook_names[approach_name],
            config.MAX_EXECUTION_TIME,
            dbutils,
            {'params': json.dumps(param_config)}
        )
        for param_config in param_configs
    ]

    res = dispatching.parallel_notebooks(notebooks, 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### t-SNE

# COMMAND ----------

approach_name = 'tSNE'

# COMMAND ----------

# List the parameters
param_configs = utils.grid_search(config.dimensionality_reduction_hyperparameters[approach_name])
param_configs

# COMMAND ----------

# Execute
if approach_name in parts_to_run:
    notebooks = [
        dispatching.NotebookData(
            config.notebook_names[approach_name],
            config.MAX_EXECUTION_TIME,
            dbutils,
            {'params': json.dumps(param_config)}
        )
        for param_config in param_configs
    ]

    res = dispatching.parallel_notebooks(notebooks, 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### UMAP

# COMMAND ----------

approach_name = 'UMAP'

# COMMAND ----------

# List the parameters
param_configs = utils.grid_search(config.dimensionality_reduction_hyperparameters[approach_name])
param_configs

# COMMAND ----------

# Execute
if approach_name in parts_to_run:
    notebooks = [
        dispatching.NotebookData(
            config.notebook_names[approach_name],
            config.MAX_EXECUTION_TIME,
            dbutils,
            {'params': json.dumps(param_config)}
        )
        for param_config in param_configs
    ]

    res = dispatching.parallel_notebooks(notebooks, 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overall Results

# COMMAND ----------

results_df = spark.read.format('delta').load(config.log_paths['dimensionality'])
results_df.display()

# COMMAND ----------

all_dr_runs = [row[0] for row in results_df.select('unique_id').collect()]
all_dr_runs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clustering
# MAGIC
# MAGIC - unique_id
# MAGIC - dr_id
# MAGIC - algorithm_type
# MAGIC - hparam_dict (if relevant)
# MAGIC - num_clusters

# COMMAND ----------

# MAGIC %md
# MAGIC ### K-Means

# COMMAND ----------

approach_name = 'kmeans'

# COMMAND ----------

# Execute
if approach_name in parts_to_run:
    notebooks = [
        dispatching.NotebookData(
            config.notebook_names[approach_name],
            config.MAX_EXECUTION_TIME,
            dbutils,
            {'params': json.dumps({}), 'dr_id': dr_run}
        )
        for dr_run in all_dr_runs
    ]

    res = dispatching.parallel_notebooks(notebooks, 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### HDBSCAN

# COMMAND ----------

approach_name = 'HDBSCAN'

# COMMAND ----------

# List the parameters
param_configs = utils.grid_search(config.clustering_hyperparameters[approach_name])
param_configs

# COMMAND ----------

# Execute
if approach_name in parts_to_run:
    notebooks = [
        dispatching.NotebookData(
            config.notebook_names[approach_name],
            config.MAX_EXECUTION_TIME,
            dbutils,
            {'params': json.dumps(param_config), 'dr_id': dr_run}
        )
        for dr_run in all_dr_runs
        for param_config in param_configs
    ]

    res = dispatching.parallel_notebooks(notebooks, 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTICS

# COMMAND ----------

approach_name = 'OPTICS'

# COMMAND ----------

# List the parameters
param_configs = utils.grid_search(config.clustering_hyperparameters[approach_name])
param_configs

# COMMAND ----------

# Execute
if approach_name in parts_to_run:
    notebooks = [
        dispatching.NotebookData(
            config.notebook_names[approach_name],
            config.MAX_EXECUTION_TIME,
            dbutils,
            {'params': json.dumps(param_config), 'dr_id': dr_run}
        )
        for dr_run in all_dr_runs
        for param_config in param_configs
    ]

    res = dispatching.parallel_notebooks(notebooks, 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overall Results

# COMMAND ----------

results_df = spark.read.format('delta').load(config.log_paths['clustering'])
results_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cluster Evaluation

# COMMAND ----------

# Execute
if 'evaluation' in parts_to_run:
    notebooks = [
        dispatching.NotebookData(
            config.notebook_names['stats'],
            config.MAX_EXECUTION_TIME,
            dbutils,
            {'unique_id': row.unique_id, 'dr_id': row.dr_id}
        )
        for row in results_df.collect()
    ]

    res = dispatching.parallel_notebooks(notebooks, 1)

# COMMAND ----------

results_df = spark.read.format('delta').load(config.log_paths['stats'])
results_df.display()
