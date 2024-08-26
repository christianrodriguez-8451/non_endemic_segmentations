# Databricks notebook source
import pyspark.sql.functions as f

from effodata import ACDS

# COMMAND ----------

acds = ACDS(use_sample_mart=True)

# COMMAND ----------

restriction_path = 'abfss://acds@sa8451posprd.dfs.core.windows.net/lookups/mhmd_lkup/mhmd_lkup.csv'
restrictions = spark.read.option('header', True).csv(restriction_path).withColumnRenamed('PID_FYT_SUB_COM_CD', 'pid_fyt_sub_com_cd')
restrictions.display()

# COMMAND ----------

restricted_info = (
    acds.products
    # .filter(f.col('fyt_pmy_dpt_cct_dsc_tx') != 'XX NO PMY DPT DESC')
    .select(
        'pid_fyt_pmy_dpt_dsc_tx', 
        'pid_fyt_rec_dpt_dsc_tx', 
        'pid_fyt_sub_dpt_dsc_tx', 
        'pid_fyt_com_dsc_tx',
        'pid_fyt_sub_com_dsc_tx',
        'pid_fyt_sub_com_cd'
    )
    .distinct()
    .join(restrictions, on='pid_fyt_sub_com_cd', how='inner')
)
restricted_info.display()

# COMMAND ----------

acds.products.filter(f.col('pid_fyt_rec_dpt_dsc_tx') == 'HBC').count()

# COMMAND ----------

acds.products.filter(f.col('pid_fyt_rec_dpt_dsc_tx') == 'HBC').join(restrictions, on='pid_fyt_sub_com_cd', how='left_anti').count()

# COMMAND ----------

acds.products.filter(f.col('pid_fyt_rec_dpt_dsc_tx') == 'HBC').select('pid_fyt_sub_com_cd').distinct().count()

# COMMAND ----------

acds.products.filter(f.col('pid_fyt_rec_dpt_dsc_tx') == 'HBC').join(restrictions, on='pid_fyt_sub_com_cd', how='left_anti').select('pid_fyt_sub_com_cd').distinct().count()

# COMMAND ----------

prod_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing_c2v/cluster_views/products_aug'
unrestricted_prods = spark.read.parquet(prod_path)

# COMMAND ----------

(
    unrestricted_prods
    .join(
        restrictions.withColumn('is_restricted', f.lit(1)),
        on='pid_fyt_sub_com_cd',
        how='left'
    )
    .fillna(0, 'is_restricted')
    .groupBy('group')
    .agg(
        f.sum(f.col('is_restricted')).alias('num_restricted'),
        f.count('*').alias('total_num')
    )
    .withColumn('prop_restricted', f.col('num_restricted') / f.col('total_num'))
    .display()
)

# COMMAND ----------

unrestricted_prods.join(restrictions, on='pid_fyt_sub_com_cd', how='left_anti').select('pid_fyt_sub_com_cd').distinct().count()
