# Databricks notebook source
# MAGIC %md
# MAGIC # NGR - GPU Optimized
# MAGIC
# MAGIC General Process:
# MAGIC
# MAGIC Inputs: a target UPC list (sub-commodity, etc.)
# MAGIC
# MAGIC 1. Pull the embeddings for the UPC list
# MAGIC 2. Find the households which have the highest scores for those UPCs
# MAGIC    - How to combine? Top k per UPC, score threshold, etc.
# MAGIC 3. Find the products most strongly associated with the household universe
# MAGIC    - Again, how to quantify? Over-indexing, average, etc.
# MAGIC
# MAGIC Challenges:
# MAGIC - HH embeddings is 60Mx100, UPC embeddings is 600kx100, run into memory constraints

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %pip install effo_embeddings poirot

# COMMAND ----------

# Python
import numpy as np

# Spark
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType

# Enterprise
from effo_embeddings.core.nextgenrec import NextGenRec
from poirot.azure.databricks import svc_prncpl_magic_setup
from effodata import ACDS, golden_rules

# COMMAND ----------

# Authenticate service principle
svc_prncpl_magic_setup(
    scope='kv-8451-tm-media-dev',
    app_id_secret='spTmMediaDev-app-id',
    app_pw_secret='spTmMediaDev-pw'
)

# Initialize ACDS
acds = ACDS(use_sample_mart=False)
rec = NextGenRec(model_as_of="current_ngr", embd_dim_subset = 100)

# COMMAND ----------

# Parameters
memoize_db = False
batch_size = 1_000_000

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sample Sub-Commodity

# COMMAND ----------

# Filter to that commodity
filter_cond = f.col('fyt_sub_com_cct_dsc_tx') == '27101 HISPANIC BEER'

product_table = acds.products.filter(filter_cond)
upc_list = [row.con_upc_no for row in product_table.collect()]
print(f'Found {len(upc_list)} UPCs')
product_table.display()

# COMMAND ----------

# Pull product embeddings
prod_map = rec.get_product_embeddings(upc_list, memoize_db=memoize_db)
prod_map['embedding'].shape

# COMMAND ----------

prod_map['embedding'].shape

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Similar HHs
# MAGIC
# MAGIC In this section, we need to find the closest HHs to each UPC. We have a 168x100 matrix of UPCs and a 54Mx100 matrix of HHs. This is a very large matrix, and we need to be careful with how we digest it. We also need to figure out how to recombine all the HHs into a cohesive list at the end.

# COMMAND ----------

hh_universe = [d.ehhn for d in rec.ehhn_df.select("ehhn").filter(f.col('ehhn').isNotNull()).collect()]
print(f'{len(hh_universe)} total HHs')

# COMMAND ----------

hh_embeddings = rec.get_customer_embeddings(hh_universe, memoize_db=memoize_db)
hh_embeddings['embeeding'].shape

# COMMAND ----------

2+2
