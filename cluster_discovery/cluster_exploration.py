# Databricks notebook source
# MAGIC %md
# MAGIC # Cluster Exploration
# MAGIC
# MAGIC The purpose of this notebook is to perform a more qualitative exploration of a clustering result. We look at things like:
# MAGIC
# MAGIC - Distribution of commodity and subcommodity

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %pip install scikit-learn==1.3.2

# COMMAND ----------

dbutils.widgets.text('unique_id', '')

# COMMAND ----------

from functools import reduce
import numpy as np
import pandas as pd
import pickle as pkl
import sklearn as skl
import matplotlib.pyplot as plt

import pyspark.sql.functions as f
from pyspark.sql import Window

from effodata import ACDS

from lib import config, utils

# COMMAND ----------

unique_id = dbutils.widgets.get('unique_id')
unique_id

# COMMAND ----------

acds = ACDS(use_sample_mart=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Input

# COMMAND ----------

config.artifact_paths['clustering'] = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/clustering_artifacts'
config.upc_path = '/dbfs/FileStore/Users/p870220/non_endemic_cluster_testing/upc_list.pkl'

# COMMAND ----------

with open(f'{config.spark_to_file(config.artifact_paths["clustering"])}/{unique_id}.pkl', 'rb') as fo:
    approach = pkl.load(fo)

approach

# COMMAND ----------

with open(config.upc_path, 'rb') as fo:
    upc_list = pkl.load(fo)

upc_list

# COMMAND ----------

len(upc_list), len(set(upc_list))

# COMMAND ----------

# Construct DataFrame
upc_labels = spark.createDataFrame(zip(upc_list, [int(i) for i in approach.labels_]), schema=['con_upc_no', 'group'])
upc_labels.limit(20).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Commodity/Sub-Commodity Distributions

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Join to Product Table

# COMMAND ----------

product_df = (
    acds
    .products
    .join(upc_labels, on='con_upc_no', how='inner')
    .filter(f.col('group') != -1)
)

product_df.count()

# COMMAND ----------

acds.products.count(), upc_labels.count(), product_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Get Hierarchy Proportions

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

prods_grouping_df.describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. Hispanic Beer Test

# COMMAND ----------

(
    acds
    .products
    .filter(f.col('fyt_sub_com_cct_dsc_tx').endswith('HISPANIC BEER'))
    .display()
)

# COMMAND ----------

(
    product_df
    .filter(f.col('fyt_sub_com_cd') == 27101)
    .groupBy('group')
    .count()
    .display()
)

# COMMAND ----------

product_df.filter(f.col('group') == 25084).display()

# COMMAND ----------

product_df.filter(f.col('group') == 25159).display()

# COMMAND ----------

(
    product_df
    .filter(f.col('group') == 25159)
    .groupBy('fyt_sub_com_cct_dsc_tx')
    .count()
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### d. Spot Checks

# COMMAND ----------

product_df.filter(f.col('group') == 24797).display()

# COMMAND ----------

(
    product_df
    .filter(f.col('group') == 24797)
    .groupBy('fyt_sub_com_cct_dsc_tx')
    .count()
    .display()
)

# COMMAND ----------

(
    product_df
    .filter(f.col('group') == 17703)
    .groupBy('fyt_sub_com_cct_dsc_tx')
    .count()
    .orderBy('count', ascending=False)
    .display()
)

# COMMAND ----------

(
    product_df
    .filter(f.col('group') == 19392)
    .select('con_dsc_tx', 'fyt_pmy_dpt_cct_dsc_tx', 'fyt_sub_dpt_cct_dsc_tx', 'fyt_com_cct_dsc_tx', 'fyt_sub_com_cct_dsc_tx')
    .display()
)

# COMMAND ----------

(
    product_df
    .filter(f.col('group') == 19392)
    .groupBy('fyt_sub_com_cct_dsc_tx')
    .count()
    .orderBy('count', ascending=False)
    .display()
)

# COMMAND ----------

(
    product_df
    .filter(f.col('group') == 24133)
    .select(
        'con_dsc_tx', 
        'pid_fyt_sub_dpt_dsc_tx',
        'fyt_com_cct_dsc_tx', 
        'fyt_sub_com_cct_dsc_tx'
    )
    .display()
)

# COMMAND ----------

def cluster_to_text(product_df, cluster_id: int) -> str:
    return '\n'.join([
        '\t'.join([
            ''.join([c for c in getattr(row, col_name)
                     if not c.isnumeric()])
            for col_name in 
            ['con_dsc_tx', 'pid_fyt_sub_dpt_dsc_tx', 'fyt_com_cct_dsc_tx', 'fyt_sub_com_cct_dsc_tx']
        ])
        for row in product_df.filter(f.col('group') == cluster_id).collect()
    ])

print(cluster_to_text(product_df, 19392))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Product Data Investigation

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. PIM

# COMMAND ----------

azure_pim_core_by_cycle = "abfss://pim@sa8451posprd.dfs.core.windows.net/pim_core/by_cycle/cycle_date=20240113"
pim = spark.read.parquet(azure_pim_core_by_cycle)
pim.limit(20).display()

# COMMAND ----------

pim.select('upc_key', 'krogerOwnedEcommerceDescription').limit(20).display()

# COMMAND ----------

pim.count()

# COMMAND ----------

pim.count(), pim.select('upc_key').distinct().count(), pim.select('upc_key', 'krogerOwnedEcommerceDescription').distinct().count()

# COMMAND ----------

(
    pim
    .select('upc_key', 'krogerOwnedEcommerceDescription')
    .distinct()
    .groupBy('upc_key')
    .count()
    .filter(f.col('count') > 1)
    .join(pim.select('upc_key', 'krogerOwnedEcommerceDescription').distinct(), on='upc_key', how='left')
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Pinto

# COMMAND ----------

pinto_path = 'abfss://data@sa8451entlakegrnprd.dfs.core.windows.net/source/third_party/prd/pinto/pinto_effo_kroger_export_20240114'
pinto = spark.read.parquet(pinto_path)
pinto.limit(20).display()

# COMMAND ----------

pinto.select('upc', 'upcs', 'name').limit(20).display()

# COMMAND ----------

pinto.select(f.explode(f.col('upcs.standard')).alias('gtin_no')).display()

# COMMAND ----------

(
    pinto
    .select(f.explode(f.col('upcs.standard')).alias('gtin_no'),
            'name')
    .distinct()
    .groupBy('gtin_no')
    .count()
    .filter(f.col('count') > 1)
    .join(
        pinto
        .select(f.explode(f.col('upcs.standard')).alias('gtin_no'),
                'name'),
        on='gtin_no',
        how='left'
    )
    .display()
)

# COMMAND ----------

(
    pinto
    .withColumn('upc1', f.explode(f.col('upcs.standard')))
    .withColumn('upc2', f.expr("substring(upc1, 1, length(upc1)-1)"))
    .withColumn('upc_len', f.length(f.col('upc2')))
    .select('upc1', 'upc2', 'upc_len')
    .groupBy('upc_len')
    .count()
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. Join

# COMMAND ----------

# Join our stuff
extras_df = (
    product_df
    .join(
        pim.select(
            f.col('upc_key').alias('con_upc_no'),
            f.col('krogerOwnedEcommerceDescription').alias('pim_description'),
        ),
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

extras_df.limit(20).display()

# COMMAND ----------

extras_df.count()

# COMMAND ----------

product_df.count(), product_df.select('con_upc_no').distinct().count()

# COMMAND ----------

(
    extras_df
    .withColumn('pim_null', f.col('pim_description').isNull())
    .withColumn('pinto_null', f.col('pinto_description').isNull())
    .groupBy('pim_null', 'pinto_null')
    .count()
    .display()
)

# COMMAND ----------

pim.select('upc_key').display()

# COMMAND ----------

pinto.select(f.explode(f.col('upcs.standard')).alias('gtin_no')).select().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Filtering

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Initial Distribution
# MAGIC
# MAGIC Look at the initial distribution.

# COMMAND ----------

initial_distr = (
    extras_df
    .groupBy('group')
    .count()
    .toPandas()
)
initial_distr

# COMMAND ----------

plt.hist(
    initial_distr['count'],
    bins=30,
    log=True
)

# COMMAND ----------

plt.hist(
    initial_distr['count'][initial_distr['count'] < 120000],
    bins=30,
    log=True
)

# COMMAND ----------

prods_grouping_pdf = prods_grouping_df.filter(f.col('num_upcs') < 100000).toPandas()
prods_grouping_df.printSchema()

# COMMAND ----------

plt.scatter(prods_grouping_pdf['num_upcs'], prods_grouping_pdf['num_department'])

# COMMAND ----------

plt.scatter(prods_grouping_pdf['num_upcs'], prods_grouping_pdf['num_subcommodity'])

# COMMAND ----------

shallow_subcommodity = (
    prods_grouping_df
    .filter(f.col('num_subcommodity') < 30)
    .toPandas()
)

plt.scatter(shallow_subcommodity['num_upcs'], shallow_subcommodity['num_subcommodity'])

# COMMAND ----------

single_subcommodity = (
    prods_grouping_df
    .filter(f.col('num_subcommodity') == 1)
    .toPandas()
)
plt.hist(single_subcommodity['num_upcs'], log=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Goldilocks Zone
# MAGIC
# MAGIC Try to find clusters in a "Goldilocks zone"

# COMMAND ----------

in_bounds = lambda col, min, max: (f.col(col) >= min) & (f.col(col) <= max)

goldilocks_clusters = (
    prods_grouping_df
    .filter(
        in_bounds('num_upcs', 50, 200) &
        in_bounds('num_subcommodity', 2, 30)
    )
)
goldilocks_clusters.display()

# COMMAND ----------

(
    extras_df
    .filter(f.col('group') == 17703)
    .filter(f.col('fyt_sub_com_cct_dsc_tx') == '36019 BOURBON/TN WHISKEY')
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. The Purge

# COMMAND ----------

w = Window.partitionBy('group').orderBy(f.desc('num_upcs'))

group_subcomm = (
    extras_df
    .groupBy('group', 'fyt_sub_com_cd')
    .agg(f.count('*').alias('num_upcs'))
)

subcomm_thresh = (
    group_subcomm
    .withColumn('rank', f.rank().over(w))
    .filter(f.col('rank') == 1)
    .withColumn('threshold', f.round(f.col('num_upcs')*.1))
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

extras_purged = (
    extras_df
    .join(
        subcomm_purged,
        on=['group', 'fyt_sub_com_cd'],
        how='inner'
    )
)

extras_purged.limit(20).display()

# COMMAND ----------

group_subcomm.count(), subcomm_thresh.count(), subcomm_purged.count()

# COMMAND ----------

extras_df.count(), extras_purged.count()

# COMMAND ----------

prods_grouping_purged_df = (
    extras_purged
    .groupBy('group')
    .agg(
        f.count('*').alias('num_upcs'),
        *[f.countDistinct(v).alias(f'num_{k}') for k, v in aggs.items()]
    )
    .select('group', 'num_upcs', *[f'num_{k}' for k in aggs],
            *[(f.col('num_upcs') / f.col(f'num_{k}')).alias(f'upcs_per_{k}') for k in aggs],
            *[(1 / f.col(f'num_{k}')).alias(f'{k}_upc_prop') for k in aggs])
)

prods_grouping_purged_df.display()

# COMMAND ----------

in_bounds = lambda col, min, max: (f.col(col) >= min) & (f.col(col) <= max)

goldilocks_clusters = (
    prods_grouping_purged_df
    .filter(
        in_bounds('num_upcs', 50, 200) &
        in_bounds('num_subcommodity', 2, 30)
    )
)
goldilocks_clusters.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Span Work

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Get Distances for Goldilocks Clusters

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

exhaustive_pairwise_distance(extras_purged.filter(f.col('group') == 7710)).limit(20).display()

# COMMAND ----------

out = []

cluster_rows = ['group'] + [col for col in goldilocks_clusters.columns if 'num_' in col]
out_rows = ['avg_distance', 'std_distance']

for row in goldilocks_clusters.collect():
    dist = exhaustive_pairwise_distance(extras_purged.filter(f.col('group') == row.group)).first()
    print(row.group, dist)
    out.append((
        *[getattr(row, col) for col in cluster_rows],
        *[getattr(dist, col) for col in out_rows]
    ))

out_df = spark.createDataFrame(out, schema=cluster_rows+out_rows)
out_df.display()

# COMMAND ----------

out

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Spot Checks

# COMMAND ----------

# Mexican Sweet Treats?
extras_df.filter(f.col('group') == 23154).display()

# COMMAND ----------

print(cluster_to_text(product_df, 23154))

# COMMAND ----------

extras_purged.filter(f.col('group') == 23692).display()

# COMMAND ----------

(
    extras_purged
    .filter(f.col('group') == 23692)
    .groupBy('fyt_sub_com_cct_dsc_tx')
    .count()
    .display()
)

# COMMAND ----------

extras_purged.filter(f.col('group') == 24444).display()

# COMMAND ----------

print(cluster_to_text(product_df, 24444))

# COMMAND ----------

(
    extras_purged
    .filter(f.col('group') == 24444)
    .groupBy('fyt_sub_com_cct_dsc_tx')
    .count()
    .display()
)

# COMMAND ----------


