# Databricks notebook source
# MAGIC %md
# MAGIC # K-Means

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

dbutils.widgets.text('params', '')
dbutils.widgets.text('dr_id', '')

# COMMAND ----------

import json
import numpy as np
import pickle as pkl
import sklearn as skl

from lib import config, utils

# COMMAND ----------

model_name = 'kmeans'
elbow_pct = 0.01

# COMMAND ----------

param_dict = json.loads(dbutils.widgets.get('params'))
dr_id = dbutils.widgets.get('dr_id')
dr_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Input

# COMMAND ----------

vectors = np.load(f'{config.spark_to_file(config.artifact_paths["dimensionality"])}/{dr_id}.npy')
vectors.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Perform Clustering
# MAGIC
# MAGIC We will attempt to perform automatic elbow plot detection.

# COMMAND ----------

# First, compute delta
i2 = skl.cluster.KMeans(n_clusters=2).fit(vectors).inertia_
i3 = skl.cluster.KMeans(n_clusters=3).fit(vectors).inertia_

max_delta = abs(i2 - i3)
cutoff_delta = max_delta * elbow_pct
print(f'Cutoff Change: {cutoff_delta}')

# COMMAND ----------

curr_val = i3
prev_val = i2
k = 3

while abs(curr_val - prev_val) > cutoff_delta:
    k += 1
    prev_val = curr_val
    kmeans = skl.cluster.KMeans(n_clusters=k).fit(vectors)
    curr_val = kmeans.inertia_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Out

# COMMAND ----------

# Add to log
unique_id = utils.generate_unique_id(model_name, param_dict) + '_' + dr_id
cluster_log_data = [(unique_id, dr_id, model_name, json.dumps(param_dict), k)]

cluster_log = spark.createDataFrame(cluster_log_data, schema=config.cluster_log_schema)
cluster_log.display()

# COMMAND ----------

cluster_log.write.mode('append').format('delta').save(config.log_paths['clustering'])

# COMMAND ----------

with open(f'{config.spark_to_file(config.artifact_paths["clustering"])}/{unique_id}.pkl', 'wb') as fo:
    pkl.dump(kmeans, fo)
