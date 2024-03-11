# Databricks notebook source
# MAGIC %md
# MAGIC # Home Segment Investigation

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %pip install fuzzywuzzy

# COMMAND ----------

from fuzzywuzzy import fuzz
from functools import reduce

import pyspark.sql.functions as f
from pyspark.sql.types import DoubleType

from effodata import ACDS

# COMMAND ----------

def match_string(s1, s2):
    val = fuzz.token_sort_ratio(s1, s2)
    return val
    
MatchUDF = udf(match_string, DoubleType())

# COMMAND ----------

acds = ACDS(use_sample_mart=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Simple Keyword Match

# COMMAND ----------

products_aug_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/cluster_views/products_aug'
products_aug = spark.read.parquet(products_aug_path)
products_aug.limit(20).display()

# COMMAND ----------

keywords = ['paint', 'tool', 'wall', 'floor', 'furniture', 'electric', 'decor', 'house', 'home']

fields = [
    'con_dsc_tx', 
    'fyt_pmy_dpt_cct_dsc_tx',
    'fyt_rec_dpt_cct_dsc_tx',
    'fyt_sub_dpt_cct_dsc_tx',
    'fyt_com_cct_dsc_tx',
    'fyt_sub_com_cct_dsc_tx',
    'pim_description',
    'pinto_description'
]

mega_cond = reduce(
    lambda a, b: a | b, 
    [f.lower(f.col(col_name)).contains(keyword)
     for col_name in fields
     for keyword in keywords]
)
keyword_filtered = products_aug.filter(mega_cond).select('bas_con_upc_no', *fields)
print(f'{keyword_filtered.count()} products returned')
keyword_filtered.display()

# COMMAND ----------

(
    keyword_filtered
    .groupBy('fyt_com_cct_dsc_tx')
    .count()
    .display()
)

# COMMAND ----------

for row in (
    keyword_filtered
    .groupBy('fyt_com_cct_dsc_tx')
    .count()
    .collect()
):
    print(row.fyt_com_cct_dsc_tx)

# COMMAND ----------

for row in (
    keyword_filtered
    .groupBy('fyt_sub_dpt_cct_dsc_tx')
    .count()
    .collect()
):
    print(row.fyt_sub_dpt_cct_dsc_tx)

# COMMAND ----------

chatgpt_res = [
    '828 FURNITURE',
    '822 PLUMBING',
    '894 GARDEN TOOLS/HDW',
    '824 PAINT',
    '877 LAMP/SHADES',
    '883 POWER TOOL/ACCSSR',
    '909 VIDEO ACCESSORIES',
    '887 APPLIANCES',
    '858 WALL DECOR AND FRAMES',
    '879 GRILLS',
    '915 HOME APPLIANCES',
    '899 HOME ENVIRONMENT',
    '942 PLANTERS/ACCESSORIES',
    '892 LIGHTING',
    '811 GARDEN CHEMICALS',
    '877 LAMP/SHADES',
    '884 HAND TOOLS',
    '913 VIDEO HARDWARE',
    '621 FLORAL - HARD GOODS',
    '739 FOOD STORAGE',
    '943 LAWN AND GARDEN',
    '688 HORTICULTURE ITEMS',
    '895 OUTDOOR LIVING',
    '798 OUTDOOR FURNITURE',
    '819 PATIO ACCESSORIES',
    '824 PAINT',
]

# COMMAND ----------

chatgpt_sub_dep = [
    '81 STORAGE',
    '77 HOME IMPRVMNT',
    '86 FURN/HOME DEC',
    '41 GARDEN-OUTDOOR',
    '80 PAINT/DECOR',
    '77 HDWE TOOLS',
    '66 ELEC/PLUMBG',
    '27 FLORAL WRAP',
    '30 CNTRL STR SPLY',
]

# COMMAND ----------

(
    keyword_filtered
    .filter(f.col('fyt_com_cct_dsc_tx').isin(chatgpt_res))
    .groupBy('fyt_sub_dpt_cct_dsc_tx')
    .count()
    .display()
)

# COMMAND ----------

(
    keyword_filtered
    .filter(f.col('fyt_sub_dpt_cct_dsc_tx').isin(chatgpt_sub_dep))
    .groupBy('fyt_sub_dpt_cct_dsc_tx')
    .count()
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Compare with Clusters

# COMMAND ----------

products_grouping_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/cluster_views/prods_grouping_df'
products_grouping = spark.read.parquet(products_grouping_path)
products_grouping.limit(20).display()

# COMMAND ----------

group_overlap = (
    products_aug
    .join(products_grouping, on='group', how='inner')
    .filter(f.col('fyt_sub_dpt_cct_dsc_tx').isin(chatgpt_sub_dep))
    .groupBy('group', 'num_upcs')
    .count()
    .withColumnRenamed('count', 'num_target_upcs')
    .withColumn('prop_cluster', f.col('num_target_upcs') / f.col('num_upcs'))
    .orderBy('prop_cluster', ascending=False)
)
group_overlap.display()

# COMMAND ----------

(
    group_overlap
    .filter((f.col('prop_cluster') > 0.7) & (f.col('num_upcs') > 100))
    .display()
)

# COMMAND ----------

(
    products_aug
    .filter(f.col('group') == 24994)
    .select(*fields)
    .display()
)

# COMMAND ----------


