# Databricks notebook source
# MAGIC %md
# MAGIC # UMAP

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %pip install umap-learn

# COMMAND ----------

dbutils.widgets.text('params', '')

# COMMAND ----------

import json
import numpy as np
import pickle as pkl
import sklearn as skl
from umap import UMAP

from lib import config, utils

# COMMAND ----------

model_name = 'UMAP'

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

# First, perform t-SNE and get the results
umap = UMAP(**param_dict)
vectors_mod = umap.fit_transform(vectors)
vectors_mod.shape

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
    pkl.dump(umap, fo)
