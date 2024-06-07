# Databricks notebook source
# MAGIC %md
# MAGIC # NGR Duplicate Investigation

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %pip install effo_embeddings

# COMMAND ----------

from effo_embeddings.core.nextgenrec import NextGenRec

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Loading

# COMMAND ----------

rec = NextGenRec(model_as_of='current_ngr')
universe = rec.get_product_embeddings()

gtin_nos = universe['domupc']
bias = universe['bias']
vectors = universe['embedding']

print(gtin_nos[:20])
print(f'{len(gtin_nos)} total UPCs')

# COMMAND ----------

# vectors.shape
universe

# COMMAND ----------

rec.rbp_df.limit(20).display()

# COMMAND ----------

len(set(gtin_nos))

# COMMAND ----------

import numpy as np

np.sum(vectors[:50], axis=1)

# COMMAND ----------

gtin_nos[:50]

# COMMAND ----------

pip freeze
