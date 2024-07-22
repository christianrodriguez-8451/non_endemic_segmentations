# Databricks notebook source
# MAGIC %md
# MAGIC # Flavored Vodka Cluster Comparison
# MAGIC
# MAGIC Overall Takeaways:
# MAGIC
# MAGIC - Lots of small (<10 UPCs) clusters which are fully flavored vodka
# MAGIC - 2 larger (30-50 UPC) clusters which are predominantly flavored vodka. They seem to correspond to Smirnoff and Burnett's respectively.
# MAGIC - 62 flavored UPCs in the 140k UPC mega-cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Data

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

products_aug_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/cluster_views/products_aug'
products_aug = spark.read.parquet(products_aug_path)
products_aug.limit(20).display()

# COMMAND ----------

cluster_metadata_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/cluster_views/prods_grouping_df'
cluster_metadata = spark.read.parquet(cluster_metadata_path)
cluster_metadata.limit(20).display()

# COMMAND ----------

vodka_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/flavored_vodka_poc'
vodka = spark.read.parquet(vodka_path)
vodka.limit(20).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Join

# COMMAND ----------

joined = (
    vodka
    .filter(f.col('overall_flag'))
    .join(products_aug, on='con_upc_no', how='inner')
)

joined.groupBy('group').count().display()

# COMMAND ----------

backfill =(
    joined
    .groupBy('group')
    .agg(f.count('*').alias('num_flavored_vodka'))
    .join(cluster_metadata, on='group', how='inner')
    .withColumn('prop_flavored_vodka', f.col('num_flavored_vodka') / f.col('num_upcs'))
)

backfill.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Spot Checks

# COMMAND ----------

(
    products_aug
    .filter(f.col('group') == 10624)
    .display()
)

# COMMAND ----------

(
    products_aug
    .filter(f.col('group') == 17549)
    .display()
)
