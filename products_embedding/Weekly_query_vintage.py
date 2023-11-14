# Databricks notebook source
# Import service principal definitions functions, date calculation and hard coded configs from config
import resources.config as config
import src.utils as utils
import products_embedding.src.date_calcs as dates

# COMMAND ----------

fw = dates.get_fw(1)
start_date = dates.get_start_date(1)
end_date = dates.get_end_date(1)

# COMMAND ----------

for modality_name in config.modality_list_nonship:
    modality_name = modality_name.replace('sales_', '')
    modality_name = modality_name.replace('/', '')
    # Pull existing vintage hh and sales data for the modality
    og_hh_vintage = \
        config.spark.read.option("mergeSchema",
                                 "true"
                                 ).option("basePath",
                                          f'{config.embedded_dimensions_dir}{config.vintages_dir}/hh_{modality_name}'
                                          ).parquet(
            f'{config.embedded_dimensions_dir}{config.vintages_dir}/hh_{modality_name}/v*'
                                                    ).where(utils.f.col('vintage_week') < fw)
    
    og_trans_agg_vintage = \
        config.spark.read.parquet(f'{config.embedded_dimensions_dir}{config.vintages_dir}/sales_{modality_name}'
                                  ).where(utils.f.col('fiscal_week') < fw)
    # Pull basket information (sales, units, visits) for the given modality from the week we want to look at

    if modality_name == 'enterprise':
        df_modality_baskets = utils.kpi.get_aggregate(
            start_date=start_date,
            end_date=end_date,
            metrics=["sales", "gr_visits", "units"],
            join_with='stores',
            apply_golden_rules=utils.golden_rules(),
            group_by=["ehhn", "trn_dt", "geo_div_no"],
        ).where(utils.f.col('ehhn').isNotNull())
    else:
        # For this to run the latest UPC list for this week, we need to know the location of the latest segment's UPC
        # list.  When a UPC list is created by the API, it writes a lookup file with the path to where the UPC list is.
        # This is a parameter passed in the API.  The logic below takes that path in the lookup file, pulls out segment,
        # traverses up two levels, and then looks for the latest directory publish.  Then it takes the latest upc list
        # of the segment to run the vintage.  All this looks like a good opportunity for a function
        upc_list_path_location = \
            config.spark.read.format("delta"
                                     ).load(
                f'{config.embedded_dimensions_dir}{config.vintages_dir}/hh_{modality_name}/upc_list_path_lookup')
        upc_list_path_url = upc_list_path_location.select(upc_list_path_location.path)
        upc_list_path_url = upc_list_path_url.rdd.map(lambda column: column.path).collect()
        upc_list_path_url = " ".join(upc_list_path_url)
        segment = [utils.Path(upc_list_path_url).parts[-1]]
        segment = " ".join(segment)
        upc_list_path_url_root = utils.Path(upc_list_path_url).parents[1]
        upc_list_path_url_root = \
            config.get_latest_modified_directory(upc_list_path_url_root.as_posix().replace('abfss:/', 'abfss://'))
        upc_list_path_url_newest = f'{upc_list_path_url_root}{segment}'
        try:
            upc_latest_location = config.spark.read.format("delta").load(upc_list_path_url_newest)
            print(upc_list_path_url_newest + "exists and will be published")
        except:
            print(upc_list_path_url_newest + " doesn't exist and needs to be skipped")
            config.modality_list_nonship.next()
            continue
        
        df_modality_baskets = utils.kpi.get_aggregate(
            start_date=start_date,
            end_date=end_date,
            metrics=["sales", "gr_visits", "units"],
            join_with='stores',
            apply_golden_rules=utils.golden_rules(),
            group_by=["ehhn", "trn_dt", "geo_div_no"],
            filter_by=utils.Sifter(upc_latest_location, join_cond=utils.Equality("gtin_no"), method="include")
        ).where(utils.f.col('ehhn').isNotNull())
  
    # Aggregate by hh and fiscal week to get weekly sales, units, and visits data for the modality
    trans_agg = \
        (df_modality_baskets.join(dates.dates_tbl, 'trn_dt', 'inner'
                                  ).groupBy('ehhn', 'fiscal_week', 'fiscal_month', 'fiscal_quarter', 'fiscal_year'
                                            ).agg(utils.f.sum('sales'
                                                               ).alias('weekly_sales'
                                                                       ),
                                                  utils.f.sum('units').alias('weekly_units'),
                                                  utils.f.sum('gr_visits').alias('weekly_visits')))

    # Find the minimum visit date in our new data pull
    ehhn_min_dt = df_modality_baskets.groupBy('ehhn').agg(utils.f.min('trn_dt').alias('trn_dt'))
    # Find the division associated with each hh's first shop in our pull
    ehhn_div_min = df_modality_baskets.join(ehhn_min_dt, how='inner', on=['ehhn', 'trn_dt'])

    # Get each household's vintage division (assuming this is their first shop in the modality, we check that later),
    # along with the min fiscal week
    div_agg = \
        (ehhn_div_min.join(dates.dates_tbl, 'trn_dt', 'inner'
                           ).orderBy('trn_dt'
                                     ).groupBy('ehhn'
                                               ).agg(utils.f.first('geo_div_no'
                                                                    ).alias('vintage_div'
                                                                            ),
                                                     utils.f.min('fiscal_week').alias('fiscal_week')))
  
    # Find households that are in the new data who don't have a prior vintage, and get their vintage week
    # (still called fiscal week at this stage)
    og_hh_vintage_ehhn = og_hh_vintage.select('ehhn')
    new_hhs = trans_agg.join(og_hh_vintage_ehhn, 'ehhn', 'leftanti').where(utils.f.col('weekly_visits') > 0)
    hhs_min_week = (new_hhs.groupBy('ehhn').agg(utils.f.min('fiscal_week').alias('fiscal_week')))
  
    # Pull all vintage information for new hhs
    hh_vintage = \
        (hhs_min_week.join(dates.dates_tbl,
                           'fiscal_week',
                           'inner'
                           ).join(div_agg,
                                  how='inner',
                                  on=['ehhn', 'fiscal_week']
                                  ).select('ehhn',
                                           'fiscal_week',
                                           'fiscal_year',
                                           'fiscal_month',
                                           'fiscal_quarter',
                                           'vintage_div'
                                           ).distinct(

        ).withColumnRenamed('fiscal_week',
                            'vintage_week'
                            ).withColumnRenamed('fiscal_year',
                                                'vintage_year'
                                                ).withColumnRenamed('fiscal_month',
                                                                    'vintage_period'
                                                                    ).withColumnRenamed('fiscal_quarter',
                                                                                        'vintage_quarter'))
    # Test to see if this is just one week
    hh_vintage.select('vintage_week').distinct().count()
    # Write out new week of vintage data
    hh_vintage.repartition(1
                           ).write.mode(
        'overwrite'
    ).parquet(f'{config.embedded_dimensions_dir}{config.vintages_dir}/hh_{modality_name}/vintage_week={int(fw)}')
  
    # Read in what we just wrote to get to weekly transaction data
    new_hh_vintage = \
        config.spark.read.parquet(
            f'{config.embedded_dimensions_dir}{config.vintages_dir}/hh_{modality_name}/vintage_week=*')
    # Make transaction level dataset
    trans_agg_vintage = trans_agg.join(new_hh_vintage, 'ehhn', 'inner')
    # Write out vintage sales dataset
    trans_agg_vintage.repartition(
        1
    ).write.mode(
        'overwrite'
    ).parquet(f'{config.embedded_dimensions_dir}{config.vintages_dir}/sales_{modality_name}/fiscal_week={int(fw)}')

    print(f'Modality {modality_name} completed for FW {fw}')
