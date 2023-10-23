# Databricks notebook source
from pathlib import Path
import resources.config as config
config.spark.conf.set("spark.sql.shuffle.partitions", "auto")
import products_embedding.src.date_calcs as dates
import src.utils as utils

from pyspark.sql.functions import collect_list

# COMMAND ----------

config.dbutils.widgets.text("upc_list_path_api", "")
upc_list_path_api = config.dbutils.widgets.get("upc_list_path_api")

# Set path for product vectors
if upc_list_path_api:
    diet_query_embeddings_directories_list = [utils.Path(upc_list_path_api).parts[-1]]
    data = [{"path": upc_list_path_api}]
    upc_list_path_lookup = config.spark.createDataFrame(data)
else:
    upc_list_path = config.get_latest_modified_directory(config.embedded_dimensions_dir + config.diet_query_dir)
    diet_query_embeddings_directories = config.spark.createDataFrame(list(config.dbutils.fs.ls(upc_list_path)))
    diet_query_embeddings_directories = \
        diet_query_embeddings_directories.filter(diet_query_embeddings_directories.name !=
                                                 'embedding_sentence_query_lookup/')
    upc_list_path_lookup = config.spark.createDataFrame(list(config.dbutils.fs.ls(upc_list_path)))
    diet_query_embeddings_directories_list = diet_query_embeddings_directories.rdd.map(lambda column:
                                                                                       column.name).collect()

# COMMAND ----------

# The KPI aggregate query will filter based on the upc list passed, all Golden Rules will apply

for directory_name in diet_query_embeddings_directories_list:
    try:
        config.fs.ls(config.embedded_dimensions_dir + config.vintages_dir + "/hh_" + directory_name)
        print(config.embedded_dimensions_dir +
              config.vintages_dir + "/hh_" + directory_name + "exists and DOESN'T need to be created")
    except:
        print(config.embedded_dimensions_dir + config.vintages_dir +
              "/hh_" + directory_name + " doesn't exist and needs to be created")

        if upc_list_path_api:
            upc_vectors_path = upc_list_path_api
        else:
            upc_vectors_path = upc_list_path + directory_name

        upc_vectors_dot_product = config.spark.read.format("delta").load(upc_vectors_path)

        directory_name = directory_name.replace('/', '')

        fiscal_st_dt = dates.today_year_ago
        fiscal_end_dt = dates.today
        modality_acds = directory_name
        modality_name = modality_acds.lower()

        acds = utils.ACDS(use_sample_mart=False)
        kpi = utils.KPI(use_sample_mart=False)
        dates_tbl = utils.acds.dates.select('trn_dt', 'fiscal_week', 'fiscal_month', 'fiscal_quarter', 'fiscal_year')

        df_ecomm_baskets = kpi.get_aggregate(
          start_date=fiscal_st_dt,
          end_date=fiscal_end_dt,
          metrics=["sales", "gr_visits", "units"],
          join_with='stores',
          apply_golden_rules=utils.golden_rules(),
          group_by=["ehhn", "trn_dt", "geo_div_no"],
          filter_by=utils.Sifter(upc_vectors_dot_product, join_cond=utils.Equality("gtin_no"), method="include")
          )

        pickup_bsk = df_ecomm_baskets.select('ehhn',
                                             'trn_dt',
                                             'sales', 'units', 'gr_visits').filter(df_ecomm_baskets.ehhn.isNotNull())

        trans_agg = pickup_bsk.join(dates_tbl, 'trn_dt', 'inner')\
                              .groupBy('ehhn', 'fiscal_week', 'fiscal_month', 'fiscal_quarter', 'fiscal_year')\
                              .agg(utils.f.sum('sales').alias('weekly_sales'),
                                   utils.f.sum('units').alias('weekly_units'),
                                   utils.f.sum('gr_visits').alias('weekly_visits'))

        # finding division
        pickup_bsk_div = df_ecomm_baskets.select('ehhn', 'trn_dt', 'sales',
                                                 'geo_div_no').filter(df_ecomm_baskets.ehhn.isNotNull())
        ehhn_min_dt = pickup_bsk_div.groupBy('ehhn').agg(utils.f.min('trn_dt').alias('trn_dt'))
        ehhn_div_min = pickup_bsk_div.join(ehhn_min_dt, how='inner', on=['ehhn', 'trn_dt'])

        div_agg = \
            ehhn_div_min.join(dates_tbl,
                              'trn_dt',
                              'inner').orderBy('trn_dt'
                                               ).groupBy('ehhn'
                                                         ).agg(utils.f.first('geo_div_no'
                                                                             ).alias('vintage_div'),
                                                               utils.f.min('fiscal_week').alias('fiscal_week'))

        new_hhs = trans_agg.where(utils.f.col('weekly_visits')>0)
        hhs_min_week = new_hhs.groupBy('ehhn').agg(utils.f.min('fiscal_week').alias('fiscal_week'))

        hh_vintage = hhs_min_week.join(dates_tbl, 'fiscal_week', 'inner') \
            .join(div_agg, how='inner', on=['ehhn', 'fiscal_week']) \
            .select('ehhn', 'fiscal_week', 'fiscal_year', 'fiscal_month', 'fiscal_quarter','vintage_div')\
            .distinct()\
            .withColumnRenamed('fiscal_week', 'vintage_week')\
            .withColumnRenamed('fiscal_year', 'vintage_year')\
            .withColumnRenamed('fiscal_month', 'vintage_period')\
            .withColumnRenamed('fiscal_quarter', 'vintage_quarter')

        # tried making this an overwrite but then it deleted all weeks and overwrites the entire folder
        hh_vintage.coalesce(1).write.mode('overwrite'
                                          ).partitionBy('vintage_week').parquet(config.embedded_dimensions_dir +
                                                                                config.vintages_dir + '/hh_' +
                                                                                modality_name)

        if upc_list_path_api:
            # write look up file of upc list location
            upc_list_path_lookup.write.mode("overwrite").format("delta").save(config.embedded_dimensions_dir +
                                                                              config.vintages_dir +
                                                                              '/hh_' + directory_name +
                                                                              '/upc_list_path_lookup')
        else:
            # write look up file of upc list location
            directory_name_slash = directory_name + '/'
            upc_list_path_lookup.select(upc_list_path_lookup.path,
                                        upc_list_path_lookup.name
                                        ).where(upc_list_path_lookup.name ==
                                                directory_name_slash
                                                ).write.mode("overwrite"
                                                             ).format("delta").save(config.embedded_dimensions_dir +
                                                                                    config.vintages_dir +
                                                                                    '/hh_' +
                                                                                    directory_name +
                                                                                    '/upc_list_path_lookup')

        new_hh_vintage = config.spark.read.parquet(config.embedded_dimensions_dir +
                                                   config.vintages_dir + '/hh_' + modality_name + '/vintage_week=*')
        trans_agg_vintage = trans_agg.join(new_hh_vintage, 'ehhn', 'inner')

        trans_agg_vintage.coalesce(1).write.mode('overwrite'
                                                 ).partitionBy('fiscal_week'
                                                               ).parquet(config.embedded_dimensions_dir +
                                                                         config.vintages_dir + '/sales_' +
                                                                         modality_name)
