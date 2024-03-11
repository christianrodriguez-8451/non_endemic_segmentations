# Databricks notebook source
# MAGIC %md
# MAGIC # HDBSCAN

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %pip install scikit-learn==1.3.2

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

model_name = 'HDBSCAN'

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
# MAGIC Perform k-means.

# COMMAND ----------

hdbs = skl.cluster.HDBSCAN(**param_dict).fit(vectors)
hdbs

# COMMAND ----------

num_clusters = int(np.max(hdbs.labels_)) + 1
print(f'Found {num_clusters} clusters')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Out

# COMMAND ----------

# Add to log
unique_id = utils.generate_unique_id(model_name, param_dict) + '_' + dr_id
cluster_log_data = [(unique_id, dr_id, model_name, json.dumps(param_dict), num_clusters)]

cluster_log = spark.createDataFrame(cluster_log_data, schema=config.cluster_log_schema)
cluster_log.display()

# COMMAND ----------

cluster_log.write.mode('append').format('delta').save(config.log_paths['clustering'])

# COMMAND ----------

with open(f'{config.spark_to_file(config.artifact_paths["clustering"])}/{unique_id}.pkl', 'wb') as fo:
    pkl.dump(hdbs, fo)
