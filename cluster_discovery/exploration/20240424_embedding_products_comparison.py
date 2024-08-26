# Databricks notebook source
# MAGIC %md
# MAGIC # Embedding Products Comparison
# MAGIC
# MAGIC What fraction of the past 52 weeks' UPCs are in each embedding space?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Magic Commands

# COMMAND ----------

# MAGIC %pip install effo_embeddings

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Imports

# COMMAND ----------

import pyspark.sql.functions as f

from effodata import ACDS, golden_rules
import effo_embeddings.core.embedding as embedding
from effo_embeddings.core.nextgenrec import NextGenRec
from effo_embeddings.core import Registry, Embedding

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. Parameters

# COMMAND ----------

start_date = '2023-04-01'
end_date = '2024-03-31'
cycle_date = '20240423'

product_path = 'dbfs:/FileStore/Users/p870220/52w_products'
c2v_path = f'abfss://personloyalty@sa8451dbxadhocprd.dfs.core.windows.net/relevancy/e451_concept2vec/{cycle_date}/concept2vec.parquet'
q2c2v_path = f'abfss://personloyalty@sa8451dbxadhocprd.dfs.core.windows.net/relevancy/e451_query2concept2vec/{cycle_date}/query2concept2vec.parquet'

# COMMAND ----------

# MAGIC %md
# MAGIC ### d. Setup

# COMMAND ----------

acds = ACDS(use_sample_mart=False)
rec = NextGenRec(model_as_of="current_ngr", embd_dim_subset = 100)

# COMMAND ----------

embedding.catalogue['concept2vec_1.0']['path'] = c2v_path
embedding.catalogue['query2concept2vec_1.0']['path'] = q2c2v_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Get Past 52 Weeks

# COMMAND ----------

prev_year_prods = (
    acds.get_transactions(
        start_date=start_date,
        end_date=end_date,
        apply_golden_rules=golden_rules(),
        join_with=['products'],
    )
    .groupBy('con_upc_no', 'bas_con_upc_no', 'gtin_no')
    .agg(f.sum(f.col('net_spend_amt')).alias('total_spend'))
)
prev_year_prods.write.mode('overwrite').parquet(product_path)

# COMMAND ----------

prev_year_prods = spark.read.parquet(product_path)
print(f'UPCs: {prev_year_prods.count()}')
prev_year_prods.limit(20).display()

# COMMAND ----------

spend_threshes = [0, 1, 10, 100, 1000]

(
    prev_year_prods
    .select(*[f.when(f.col('total_spend') > thresh, 1).otherwise(0).alias(f'over_{thresh}')
              for thresh in spend_threshes])
    .agg(*[f.sum(f.col(f'over_{thresh}')).alias(f'num_over_{thresh}')
           for thresh in spend_threshes])
    .display()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Compare with Embeddings

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Concept2Vec

# COMMAND ----------

# Load the embeddings
c2v_embeddings = Embedding('concept2vec_1.0')
c2v_upcs = spark.createDataFrame(
    [[k] for k in c2v_embeddings.get_keys()],
    schema=['con_upc_no']
)
print(c2v_upcs.count())
c2v_upcs.limit(20).display()

# COMMAND ----------

c2v_prev_year = (
    prev_year_prods
    .join(
        c2v_upcs,
        on='con_upc_no',
        how='inner'
    )
)

(
    c2v_prev_year
    .select(*[f.when(f.col('total_spend') > thresh, 1).otherwise(0).alias(f'over_{thresh}')
              for thresh in spend_threshes])
    .agg(*[f.sum(f.col(f'over_{thresh}')).alias(f'num_over_{thresh}')
           for thresh in spend_threshes])
    .display()
)

# COMMAND ----------

left_anti_c2v = (c2v_upcs.join(prev_year_prods, c2v_upcs['con_upc_no'] == prev_year_prods['con_upc_no'], 'left_anti'))
left_anti_prev_year_prods = (prev_year_prods.join(c2v_upcs, prev_year_prods['con_upc_no'] == c2v_upcs['con_upc_no'], 'left_anti'))
c2v_upcs.count() #=731883
#prev_year_prods.count() #=605230

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Query2Concept2Vec
# MAGIC
# MAGIC Exact same as concept2vec

# COMMAND ----------

# Load the embeddings
q2c2v_embeddings = Embedding('query2concept2vec_1.0')
q2c2v_upcs = spark.createDataFrame(
    [[k] for k in q2c2v_embeddings.get_keys()],
    schema=['gtin_no']
)
print(q2c2v_upcs.count())
q2c2v_upcs.limit(20).display()

# COMMAND ----------

q2c2v_prev_year = (
    prev_year_prods
    .join(
        q2c2v_upcs,
        on='gtin_no',
        how='inner'
    )
)

(
    q2c2v_prev_year
    .select(*[f.when(f.col('total_spend') > thresh, 1).otherwise(0).alias(f'over_{thresh}')
              for thresh in spend_threshes])
    .agg(*[f.sum(f.col(f'over_{thresh}')).alias(f'num_over_{thresh}')
           for thresh in spend_threshes])
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. NGR

# COMMAND ----------

rec.rbp_df.limit(20).display()

# COMMAND ----------

ngr_prev_year = (
    prev_year_prods
    .select('bas_con_upc_no', 'total_spend')
    .join(
        rec.rbp_df.select('bas_con_upc_no').distinct(),
        # rec.rbp_df.select(f.col('bas_con_upc_no').alias('con_upc_no')).distinct(),
        on='bas_con_upc_no',
        how='inner'
    )
)

(
    ngr_prev_year
    .select(*[f.when(f.col('total_spend') > thresh, 1).otherwise(0).alias(f'over_{thresh}')
              for thresh in spend_threshes])
    .agg(*[f.sum(f.col(f'over_{thresh}')).alias(f'num_over_{thresh}')
           for thresh in spend_threshes])
    .display()
)

# COMMAND ----------



# COMMAND ----------

len('0005037501941')

# COMMAND ----------

c2v_upcs.filter(f.length(f.col('con_upc_no')) == 13).count()

# COMMAND ----------

q2c2v_upcs.filter(f.length(f.col('gtin_no')) == 13).count()
