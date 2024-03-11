# Databricks notebook source
# MAGIC %md
# MAGIC # Cluster Evaluation
# MAGIC
# MAGIC This notebook performs a few tests of cluster quality.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %pip install scikit-learn==1.3.2

# COMMAND ----------

dbutils.widgets.text('unique_id', '')
dbutils.widgets.text('dr_id', '')

# COMMAND ----------

import json
import numpy as np
import pickle as pkl
import sklearn as skl

from lib import config, utils

# COMMAND ----------

unique_id = dbutils.widgets.get('unique_id')
dr_id = dbutils.widgets.get('dr_id')
unique_id, dr_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Input

# COMMAND ----------

vectors_base = np.load(config.embedding_path)
vectors_base.shape

# COMMAND ----------

vectors = np.load(f'{config.spark_to_file(config.artifact_paths["dimensionality"])}/{dr_id}.npy')
vectors.shape

# COMMAND ----------

with open(f'{config.spark_to_file(config.artifact_paths["clustering"])}/{unique_id}.pkl', 'rb') as fo:
    approach = pkl.load(fo)

approach

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run Tests

# COMMAND ----------

# Get cluster sizes
labels = approach.labels_
indices, counts = np.unique(labels, return_counts=True)
list(zip(indices, counts))

# COMMAND ----------

# Stats
cluster_size_stats = {
    'min': int(np.min(counts)),
    'q1': int(np.quantile(counts, .25)),
    'median': int(np.median(counts)),
    'q3': int(np.quantile(counts, .75)),
    'max': int(np.max(counts)),
    'mean': float(np.mean(counts)),
    'stdev': float(np.std(counts))
}
cluster_size_stats

# COMMAND ----------

# Get sample size
sample_size = min(config.MAX_SILHOUETTE_SIZE, len(vectors))
print(f'Using {sample_size}/{len(vectors)} vectors for calculation')

# Silhouette
base_silhouette = float(skl.metrics.silhouette_score(vectors_base, labels, sample_size=sample_size))
dr_silhouette = float(skl.metrics.silhouette_score(vectors, labels, sample_size=sample_size))
base_silhouette, dr_silhouette

# COMMAND ----------

# Calinski-Harbasz
base_ch = float(skl.metrics.calinski_harabasz_score(vectors_base, labels))
dr_ch = float(skl.metrics.calinski_harabasz_score(vectors, labels))
base_ch, dr_ch

# COMMAND ----------

# Davies-Bouldin
base_db = float(skl.metrics.davies_bouldin_score(vectors_base, labels))
dr_db = float(skl.metrics.davies_bouldin_score(vectors, labels))
base_db, dr_db

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Out

# COMMAND ----------

stats_log_data = [(
    unique_id,
    dr_id,
    *cluster_size_stats.values(),
    base_silhouette,
    dr_silhouette,
    base_ch,
    dr_ch,
    base_db,
    dr_db
)]

stats_log = spark.createDataFrame(stats_log_data, schema=config.stats_log_schema)
stats_log.display()

# COMMAND ----------

stats_log.write.mode('append').format('delta').save(config.log_paths['stats'])
