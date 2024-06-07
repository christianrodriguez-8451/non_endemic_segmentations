# Databricks notebook source
# MAGIC %md
# MAGIC # Cluster Audiences
# MAGIC
# MAGIC Run some diagnostics for the audiences of the different clusters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scratch

# COMMAND ----------

products_aug_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/cluster_views/products_aug'
products_aug = spark.read.parquet(products_aug_path)
products_aug.limit(20).display()

# COMMAND ----------

import pyspark.sql.functions as f
(
    products_aug
    .withColumn('description_len', f.length(f.col('pim_description')))
    .select('description_len')
    .describe()
    .display()
)

# COMMAND ----------

products_grouping_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/cluster_views/prods_grouping_df'
prods_grouping = spark.read.parquet(products_grouping_path)
prods_grouping.limit(20).display()

# COMMAND ----------

prods_grouping.select(*[col for col in prods_grouping.columns if 'num_' in col]).describe().display()
