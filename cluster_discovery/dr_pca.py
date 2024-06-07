# Databricks notebook source
# MAGIC %md
# MAGIC # PCA

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

dbutils.widgets.text('params', '')

# COMMAND ----------

import json
import numpy as np
import pickle as pkl
import sklearn as skl

from lib import config, utils

# COMMAND ----------

model_name = 'pca'

# COMMAND ----------

param_dict = json.loads(dbutils.widgets.get('params'))
param_dict

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Input

# COMMAND ----------

vectors = np.load(config.embedding_path)
vectors.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Execute

# COMMAND ----------

# First, perform PCA and get the results
pca = skl.decomposition.PCA(**param_dict)
vectors_mod = pca.fit_transform(vectors)
vectors_mod.shape

# COMMAND ----------

# Get the explained variance
pca.explained_variance_

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Out

# COMMAND ----------

# Get quality score
tw = utils.get_approximate_trustworthiness(vectors, vectors_mod)
tw

# COMMAND ----------

# Add to log
unique_id = utils.generate_unique_id(model_name, param_dict)
dr_log_data = [(unique_id, model_name, json.dumps(param_dict), tw)]

dr_log = spark.createDataFrame(dr_log_data, schema=config.dr_log_schema)
dr_log.display()

# COMMAND ----------

dr_log.write.mode('append').format('delta').save(config.log_paths['dimensionality'])

# COMMAND ----------

# Save artifact
np.save(f'{config.spark_to_file(config.artifact_paths["dimensionality"])}/{unique_id}.npy', vectors_mod)

with open(f'{config.spark_to_file(config.artifact_paths["dimensionality"])}/{unique_id}.pkl', 'wb') as fo:
    pkl.dump(pca, fo)
