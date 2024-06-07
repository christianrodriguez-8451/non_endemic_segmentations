# Databricks notebook source
# MAGIC %md
# MAGIC # Duplicate UPC Investigation

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import pyspark.sql.functions as f

import sys
sys.path.append('..')
sys.path.append('../..')

from resources import config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Data

# COMMAND ----------

dbutils.fs.ls(config.product_vectors_description_path)

# COMMAND ----------

description_path = f'{config.product_vectors_description_path}/cycle_date=20240219'
descriptions_df = spark.read.load(description_path)
descriptions_df.limit(20).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Duplicate Identification

# COMMAND ----------

distinct_upcs = descriptions_df.select('gtin_no').distinct()
total_distinct = descriptions_df.distinct()

total_num = descriptions_df.count()
total_num_distinct = total_distinct.count()
num_distinct_upcs = distinct_upcs.count()

print(f'Total Count: {total_num}')
print(f'Total Distinct: {total_num_distinct}')
print(f'Distinct UPCs: {num_distinct_upcs}')
print()
print(f'Rows lost in .distinct(): {total_num - total_num_distinct}')
print(f'Distinct to UPC drop: {total_num_distinct - num_distinct_upcs}')

# COMMAND ----------

# UPC-level counts
(
    descriptions_df
    .groupBy('gtin_no')
    .count()
    .filter(f.col('count') > 1)
    .orderBy('count', ascending=False)
    .display()
)

# COMMAND ----------

# When distinct properly applied
(
    total_distinct
    .groupBy('gtin_no')
    .count()
    .filter(f.col('count') > 1)
    .orderBy('count', ascending=False)
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Spot Check

# COMMAND ----------

(
    total_distinct
    .filter(f.col('gtin_no') == '0088796138171')
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Distance Checker

# COMMAND ----------

from pyspark.sql.types import FloatType
# import pyspark.mllib.linalg.Vectors as v


dot_product_udf = f.udf(lambda a, b: sum(i*j for i, j in zip(a, b)), returnType=FloatType())
# dot_product_udf = f.udf(lambda a, b: float(a.dot(b)), returnType=FloatType())

cross_dists = (
    total_distinct.alias('a')
    .join(total_distinct.alias('b'), on='gtin_no', how='inner')
    .filter(f.col('a.vector') != f.col('b.vector'))
    .withColumn(
        'dist',
        dot_product_udf(
            f.col('a.vector'), 
            f.col('b.vector')
        )
    )
    .orderBy('dist', ascending=True)
)
cross_dists.display()

# COMMAND ----------

(
    cross_dists
    .select('gtin_no', 'dist')
    .filter(f.col('dist') < .99)
    .distinct()
    .display()
)

# COMMAND ----------

3582+617

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Spot Check 2

# COMMAND ----------

(
    total_distinct
    .filter(f.col('gtin_no') == '0000000000015')
    .display()
)
