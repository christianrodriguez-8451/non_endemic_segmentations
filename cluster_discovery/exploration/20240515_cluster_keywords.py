# Databricks notebook source
# MAGIC %md
# MAGIC # Cluster-Derived Segments

# COMMAND ----------

from collections import Counter
import pandas as pd
import pyspark.sql.functions as f

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/cluster_views')

# COMMAND ----------

unrestricted_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/cluster_views/unrestricted_pairwise'
unrestricted = spark.read.parquet(unrestricted_path)
unrestricted.display()

# COMMAND ----------

light_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/cluster_views/light_pairwise'
light = spark.read.parquet(light_path)
light.display()

# COMMAND ----------

prod_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/cluster_views/products_aug'
unrestricted_prods = spark.read.parquet(prod_path)

light_prod_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/cluster_views/light_purge'
light_prods = spark.read.parquet(light_prod_path)

# COMMAND ----------

unrestricted_prods.filter(f.col('group') == 16378).display()

# COMMAND ----------

pdf = unrestricted_prods.filter(f.col('group') == 16378).toPandas()
pdf

# COMMAND ----------

# pdf.pim_description.lower()

# COMMAND ----------

# 'abc'.replace('', '')

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
# get_most_common_keywords(pdf)

# COMMAND ----------

unrestricted_keywords = unrestricted.join(group_keywords, on='group', how='inner')
unrestricted_keywords.select('group', 'num_upcs', 'avg_distance', 'keywords').display()

# COMMAND ----------

(
    unrestricted_keywords
    .select('group', 'num_upcs', 'avg_distance', 'keywords')
    .filter(f.col('num_upcs') > 50)
    .display()
)

# COMMAND ----------



# COMMAND ----------

identified_clusters  {
    853:   'Blueberries',
    17692: 'Dog Food',
    17629: 'Hair Claws',
    24252: 'Burt\'s Bees',
}

# COMMAND ----------

unrestricted_prods.filter(f.col('group') == 17629).display()
