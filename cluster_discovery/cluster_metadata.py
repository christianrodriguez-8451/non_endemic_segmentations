# Databricks notebook source
# MAGIC %md
# MAGIC # Cluster Metadata
# MAGIC
# MAGIC The purpose of this notebook is to perform a variety of data aggregations based off of the cluster values.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %pip install scikit-learn==1.3.2

# COMMAND ----------

from functools import reduce
import numpy as np
import pandas as pd
import pickle as pkl
import sklearn as skl

import pyspark.sql.functions as f
from pyspark.sql import Window

from effodata import ACDS

from lib import config, utils

# COMMAND ----------

# Parameters
unique_id = 'HDBSCAN_0.5_5_UMAP_0_10_5'
max_num_items = 1000

cluster_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/clustering_artifacts'
upc_path = '/dbfs/FileStore/Users/p870220/non_endemic_cluster_testing/upc_list.pkl'
pim_path = "abfss://pim@sa8451posprd.dfs.core.windows.net/pim_core/by_cycle/cycle_date=20240113"
pinto_path = 'abfss://data@sa8451entlakegrnprd.dfs.core.windows.net/source/third_party/prd/pinto/pinto_effo_kroger_export_20240114'
out_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/cluster_views'

# COMMAND ----------

# Initialization
acds = ACDS(use_sample_mart=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Data

# COMMAND ----------

# Hard-coded paths
config.artifact_paths['clustering'] = cluster_path
config.upc_path = upc_path

# COMMAND ----------

# Load clustering
with open(f'{config.spark_to_file(config.artifact_paths["clustering"])}/{unique_id}.pkl', 'rb') as fo:
    approach = pkl.load(fo)

approach

# COMMAND ----------

# Load UPC list
with open(config.upc_path, 'rb') as fo:
    upc_list = pkl.load(fo)

upc_list

# COMMAND ----------

# Construct DataFrame
upc_labels = spark.createDataFrame(
    zip(upc_list, [int(i) for i in approach.labels_]), 
    schema=['con_upc_no', 'group']
)
upc_labels.limit(20).display()

# COMMAND ----------

# Debugging counts
num_labels = upc_labels.count()
num_deduped = upc_labels.distinct().count()
num_groups = upc_labels.select('group').distinct().count()
num_upcs = upc_labels.select('con_upc_no').distinct().count()
num_unsegmented = upc_labels.filter(f.col('group') == -1).count()

print(f'{num_labels} labels ({num_deduped} distinct)')
print(f'{num_upcs} UPCs in {num_groups} groups ({num_unsegmented} unsegmented)')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.Construct Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Product Table

# COMMAND ----------

# Join to the product table
product_df = (
    acds
    .products
    .join(upc_labels, on='con_upc_no', how='inner')
    .filter(f.col('group') != -1)
)

product_df.limit(20).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. PIM and Pinto

# COMMAND ----------

# Load PIM
pim = spark.read.parquet(pim_path)
pim.limit(20).display()

# COMMAND ----------

pinto = spark.read.parquet(pinto_path)
pinto.limit(20).display()

# COMMAND ----------

# Join together
products_aug = (
    product_df
    .join(
        pim.select(
            f.col('upc_key').alias('con_upc_no'),
            f.col('krogerOwnedEcommerceDescription').alias('pim_description'),
        ).distinct(),
        on='con_upc_no',
        how='left'
    )
    .join(
        pinto
        .select(
            f.explode(f.col('upcs.standard')).alias('con_upc_no'),
            f.col('name').alias('pinto_description'),
        )
        .withColumn('con_upc_no', f.expr("substring(con_upc_no, 1, length(con_upc_no)-1)"))
        .distinct(),
        on='con_upc_no',
        how='left'
    )
)
products_aug.limit(20).display()

# COMMAND ----------

# Check join integrity (only dups should be those few)
product_df.count(), products_aug.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. Cluster Metadata

# COMMAND ----------

aggs = {
    'department': 'fyt_pmy_dpt_cd',
    'recap_department': 'fyt_rec_dpt_cd',
    'sub_department': 'fyt_sub_dpt_cd',
    'commodity': 'fyt_com_cd',
    'subcommodity': 'fyt_sub_com_cd',
}

prods_grouping_df = (
    product_df
    .groupBy('group')
    .agg(
        f.count('*').alias('num_upcs'),
        *[f.countDistinct(v).alias(f'num_{k}') for k, v in aggs.items()]
    )
    .select('group', 'num_upcs', *[f'num_{k}' for k in aggs],
            *[(f.col('num_upcs') / f.col(f'num_{k}')).alias(f'upcs_per_{k}') for k in aggs],
            *[(1 / f.col(f'num_{k}')).alias(f'{k}_upc_prop') for k in aggs])
)

prods_grouping_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### d. Pruning

# COMMAND ----------

def create_pruning(products_aug, prop):
    w = Window.partitionBy('group').orderBy(f.desc('num_upcs'))

    group_subcomm = (
        products_aug
        .groupBy('group', 'fyt_sub_com_cd')
        .agg(f.count('*').alias('num_upcs'))
    )

    subcomm_thresh = (
        group_subcomm
        .withColumn('rank', f.rank().over(w))
        .filter(f.col('rank') == 1)
        .withColumn('threshold', f.round(f.col('num_upcs')*prop))
        .select('group', 'threshold', f.col('num_upcs').alias('max_upcs'))
        .distinct()
    )

    subcomm_purged = (
        group_subcomm
        .join(
            subcomm_thresh,
            on='group',
            how='inner'
        )
        .filter(f.col('num_upcs') >= f.col('threshold'))
    )

    products_purged = (
        products_aug
        .join(
            subcomm_purged,
            on=['group', 'fyt_sub_com_cd'],
            how='inner'
        )
    )

    return products_purged

light_purge = create_pruning(products_aug, .05)
heavy_purge = create_pruning(products_aug, .1)
heavy_purge.limit(20).display()

# COMMAND ----------

# Get step down
products_aug.count(), light_purge.count(), heavy_purge.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### e. Write Out

# COMMAND ----------

asset_names = {
    'product_df': product_df,
    'products_aug': products_aug,
    'prods_grouping_df': prods_grouping_df,
    'light_purge': light_purge,
    'heavy_purge': heavy_purge,
}

for name, df in asset_names.items():
    df.write.mode('overwrite').parquet(f'{out_path}/{name}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cluster Hierarchy

# COMMAND ----------

hierarchy = ['fyt_pmy_dpt_cct_dsc_tx', 'fyt_rec_dpt_cct_dsc_tx', 'fyt_sub_dpt_cct_dsc_tx', 'fyt_com_cct_dsc_tx', 'fyt_sub_com_cct_dsc_tx']
conds = [f.when(f.col(f'left.{col_name}') == f.col(f'right.{col_name}'), i+1)
         for i, col_name in enumerate(reversed(hierarchy))]
full_cond = reduce(lambda a, b: b.otherwise(a), reversed(conds), len(hierarchy)+1)

def exhaustive_pairwise_distance(upc_df):
    avg_dist = (
        upc_df.alias('left')
        .crossJoin(upc_df.alias('right'))
        .withColumn('pairwise_dist', full_cond)
        .agg(f.mean(f.col('pairwise_dist')).alias('avg_distance'),
             f.stddev(f.col('pairwise_dist')).alias('std_distance'))
    )
    return avg_dist

# COMMAND ----------

unrestricted_rows = []
light_rows = []
heavy_rows = []

cluster_rows = ['group'] + [col for col in prods_grouping_df.columns if 'num_' in col]
out_rows = ['avg_distance', 'std_distance']

for row in prods_grouping_df.filter(f.col('num_upcs') <= max_num_items).collect():
    print(f'--- Cluster {row.group} ---')

    d_unrestricted = exhaustive_pairwise_distance(
        products_aug.filter(f.col('group') == row.group)
    ).first()
    print(f'Unrestricted: {d_unrestricted}')
    d_light = exhaustive_pairwise_distance(
        light_purge.filter(f.col('group') == row.group)
    ).first()
    print(f'Light: {d_light}')
    d_heavy = exhaustive_pairwise_distance(
        heavy_purge.filter(f.col('group') == row.group)
    ).first()
    print(f'Heavy: {d_heavy}')

    unrestricted_rows.append((
        *[getattr(row, col) for col in cluster_rows],
        *[getattr(d_unrestricted, col) for col in out_rows]
    ))
    light_rows.append((
        *[getattr(row, col) for col in cluster_rows],
        *[getattr(d_light, col) for col in out_rows]
    ))
    heavy_rows.append((
        *[getattr(row, col) for col in cluster_rows],
        *[getattr(d_heavy, col) for col in out_rows]
    ))

out_unrestricted = spark.createDataFrame(
    unrestricted_rows, 
    schema=cluster_rows+out_rows
)
out_light = spark.createDataFrame(
    light_rows, 
    schema=cluster_rows+out_rows
)
out_heavy = spark.createDataFrame(
    heavy_rows, 
    schema=cluster_rows+out_rows
)

# COMMAND ----------

asset_names = {
    'unrestricted': out_unrestricted,
    'light': out_light,
    'heavy': out_heavy,
}

for name, df in asset_names.items():
    df.write.mode('overwrite').parquet(f'{out_path}/{name}_pairwise')
