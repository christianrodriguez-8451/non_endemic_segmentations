# Databricks notebook source
# MAGIC %md
# MAGIC # Cluster-Derived Segments

# COMMAND ----------

from collections import Counter
import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

# COMMAND ----------

base_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing_c2v_res/cluster_views'

# COMMAND ----------

unrestricted_path = f'{base_path}/unrestricted_pairwise'
unrestricted = spark.read.parquet(unrestricted_path)
unrestricted.display()

# COMMAND ----------

light_path = f'{base_path}/light_pairwise'
light = spark.read.parquet(light_path)
light.display()

# COMMAND ----------

prod_path = f'{base_path}/products_aug'
unrestricted_prods = spark.read.parquet(prod_path)

light_prod_path = f'{base_path}/light_purge'
light_prods = spark.read.parquet(light_prod_path)

# COMMAND ----------

def get_most_common_keywords(df):
    """Pandas UDF to extract PIM keywords."""
    stripped_names = []

    for _, row in df.iterrows():
        if row.pim_description is None:
            continue

        brand_name = row.prn_mfg_dsc_tx.lower() if row.prn_mfg_dsc_tx is not None else ''
        stripped_names.extend([
            word.strip('()[]{}™®')
            for word in row.pim_description.lower().replace(brand_name, '').split()
        ])

    stripped_names = [i for i in stripped_names if len(i) > 1]
    
    most_common = [i[0] for i in
                   sorted(Counter(stripped_names).items(), 
                          key=lambda x: -x[1])[:5]]
    
    out = pd.DataFrame({
        'group': [df.loc[0, 'group']],
        'keywords': [str(most_common)]
    })

    return out

from pyspark.sql.types import StringType, StructType, StructField, ArrayType, IntegerType

schema = StructType([
    StructField('group', IntegerType()),
    StructField('keywords', StringType()),
])

group_keywords = (
    unrestricted_prods
    .groupBy('group')
    .applyInPandas(get_most_common_keywords, schema)
)
group_keywords.display()

# COMMAND ----------


from pyspark.sql.functions import desc, row_number

window_spec = Window.partitionBy('group').orderBy(f.desc('count'))

top_commodities = (
    unrestricted_prods
    .groupBy('group', 'pid_fyt_com_dsc_tx')
    .count()
    .withColumn('row_number', f.row_number().over(window_spec))
    .filter(f.col('row_number') <= 3)
    .groupBy('group')
    .agg(f.collect_list('pid_fyt_com_dsc_tx').alias('top_commodities'))
)

top_sub_commodities = (
    unrestricted_prods
    .groupBy('group', 'pid_fyt_sub_com_dsc_tx')
    .count()
    .withColumn('row_number', f.row_number().over(window_spec))
    .filter(f.col('row_number') <= 3)
    .groupBy('group')
    .agg(f.collect_list('pid_fyt_sub_com_dsc_tx').alias('top_sub_commodities'))
)

top_commodities.display()
top_sub_commodities.display()

# COMMAND ----------

unrestricted_keywords = (
    unrestricted
    .join(group_keywords, on='group', how='inner')
    .join(top_commodities, on='group', how='inner')
    .join(top_sub_commodities, on='group', how='inner')
)
unrestricted_keywords.select('group', 'num_upcs', 'mean_dist', 'keywords', 'top_commodities', 'top_sub_commodities').display()
