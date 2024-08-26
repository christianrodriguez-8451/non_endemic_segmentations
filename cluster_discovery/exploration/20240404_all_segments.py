# Databricks notebook source
# MAGIC %pip install effo_embeddings fuzzywuzzy

# COMMAND ----------

# import logging
# logging.basicConfig(level=logging.DEBUG)

# COMMAND ----------

import effo_embeddings.core.embedding as embedding

cycle_date = '20240319'
embedding.catalogue['concept2vec_1.0']['path'] = f'abfss://personloyalty@sa8451dbxadhocprd.dfs.core.windows.net/relevancy/e451_concept2vec/{cycle_date}/concept2vec.parquet'

from effo_embeddings.core.nextgenrec import NextGenRec
from effo_embeddings.core import Registry, Embedding

# COMMAND ----------

e = Embedding('concept2vec_1.0')

# COMMAND ----------

# Python
import logging
import numpy as np
from fuzzywuzzy import fuzz

# Spark
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType

# Enterprise


from effodata import ACDS, golden_rules

# COMMAND ----------

logging.basicConfig(level=logging.DEBUG)

# COMMAND ----------

acds = ACDS(use_sample_mart=False)

# COMMAND ----------



# COMMAND ----------

help(Embedding)

# COMMAND ----------

e.model_file

# COMMAND ----------



# COMMAND ----------

acds.get_transactions(
    start_date='2023-01-01',
    end_date='2023-12-31',
    apply_golden_rules=golden_rules()
).select('gtin_no').distinct().count()

# COMMAND ----------


Registry().list_embeddings()

# COMMAND ----------

## Testing Embedding
from effo_embeddings.core import Embedding
e=Embedding('concept2vec_1.0') #create concept2vec embedding

k=e.get_keys()[:5] # get some upcs that are in the model
print('Keys:',k) 


s,_=e.get_similar_by_key(['0000000004087' ],10) # find similar upcs to upc 0000000004087 (Tomato)
print('Similar Keys',s)


vecs=e.get_vectors(['0000000004087']) # get raw vectors for the upcs
print('Vectors',vecs)


s,_=e.get_similar_by_vector(vecs,10) #search by vectors that are similar to given vectors.
print('Similar Keys',s)

# COMMAND ----------

c2v_upcs = spark.createDataFrame(
    [[k] for k in e.get_keys()],
    schema=['con_upc_no']
)
c2v_upcs.limit(20).display()

# COMMAND ----------

c2v_upcs.count()

# COMMAND ----------

acds.get_transactions(
    start_date='2023-01-01',
    end_date='2024-04-11',
    apply_golden_rules=golden_rules(),
    join_with=['product']
).select('con_upc_no').distinct().join(c2v_upcs, on='con_upc_no').count()

# COMMAND ----------

c2v_upcs.count(), c2v_upcs.join(acds.products, on='con_upc_no').count()

# COMMAND ----------

acds.products

# COMMAND ----------

e.index_to_key

# COMMAND ----------

[i for i in e.get_keys()
 if len(i) != len('0501099394759')]

# COMMAND ----------

len(e.get_keys())

# COMMAND ----------

e.get_keys()

# COMMAND ----------

import random
# from core import Embedding
random.seed(989)
e=Embedding('query2concept2vec_1.0') #create concept2vec embedding

k=random.sample(e.get_keys(), 5) # get some queries & upcs that are in the model
print('Keys:',k) 


s,_=e.get_similar_by_key(['iced tea' ],25) # find relevant(similar) upcs to query iced tea
print('Similar Keys',s)
print('Relevant UPCs',[i for i in s[0] if i.isdigit()])


vecs=e.get_vectors(['iced tea' ]) # get raw vectors for the query
print('Vectors',vecs)


s,_=e.get_similar_by_vector(vecs,10) #search by vectors that are similar to given vectors.
print('Similar Keys',s)

