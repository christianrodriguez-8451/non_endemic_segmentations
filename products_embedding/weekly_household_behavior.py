# common python packages

# spark packages
import pyspark.sql.functions as f
from pyspark.sql import Window

# import internal packages
from effodata import ACDS, golden_rules
import kayday as kd

# Import service principal definitions functions, date calculation and hard coded configs from config
import resources.config as config
import products_embedding.config as configs
import products_embedding.src.utils as utils
import products_embedding.src.pull_input_data as ingress
import products_embedding.src.date_calcs as dates
import products_embedding.src.generate_segments as segments
import products_embedding.src.hmls as hml

# COMMAND ----------

# Set configurations
for sa in config.storage_account:
    spark.conf.set(f"fs.azure.account.auth.type.{sa}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{sa}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{sa}.dfs.core.windows.net", config.service_application_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{sa}.dfs.core.windows.net", config.service_credential)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{sa}.dfs.core.windows.net", f"https://login.microsoftonline.com/{config.directory_id}/oauth2/token")

# see if these make this notebook run more efficiently
# spark.conf.set("spark.sql.shuffle.partitions", "auto")
# spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', 'true')
# spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

# this doesn't seem to work like get_spark_session... maybe refactor
acds = ACDS(use_sample_mart=False)

# get current datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta

end_week_widget = ''
write_mode = 'overwrite'

if end_week_widget == '':
    # this needs to be one week ahead of the last week to be included in the seg
    # makes sense for automation, but a little unintuitive for restatement
    # example: if we want to restate the seg for data ending 2022 P1W3, then the
    # current week value needs to be 2022 P1W4, because we will go backwards one week
    current_week = None

    # last week
    end_week = dates.get_weeks_ago_from_fw(current_week, 1)
    # gives 52 weeks of history
    start_week = dates.get_weeks_ago_from_fw(end_week, 51)
    # assign current week an actual value
    stratum_week = (kd.KrogerDate(year=int(end_week[:4]), 
                                  period=int(end_week[4:6]), 
                                  week=int(end_week[6:]), 
                                  day=1)
                    # move ahead one week for stratum
                    .ahead(weeks=1).format_week()[1:]
                   )
elif len(end_week_widget):  # == 8 and end_week_widget.isnumeric():
    # rough check that we have a valid fiscal week. KrogerDate will error if it isn't valid
    kd.KrogerDate(int(end_week_widget[:4]), int(end_week_widget[4:6]), int(end_week_widget[6:]), 1)

    # last week
    end_week = dates.get_weeks_ago_from_fw(end_week_widget, 0)
    # gives 52 weeks of history
    start_week = dates.get_weeks_ago_from_fw(end_week, 51)
    # assign current week an actual value
    stratum_week = (kd.KrogerDate(int(end_week[:4]), int(end_week[4:6]), int(end_week[6:]), 1)
                        .ahead(weeks=1)
                        .format_week()[1:]
                   )
else:
    raiseValueError("please input a valid fiscal week of the form YYYYPPWW")

# COMMAND ----------

diet_query_vintages_directories = \
    spark.createDataFrame(list(dbutils.fs.ls(config.embedded_dimensions_dir + config.vintages_dir)))
diet_query_vintages_directories = \
    diet_query_vintages_directories.filter(diet_query_vintages_directories.name.like('sales_%'))
diet_query_vintages_directories = diet_query_vintages_directories.rdd.map(lambda column: column.name).collect()
modality_list_nonship = diet_query_vintages_directories

for modality_name in modality_list_nonship:
    modality_name = modality_name.replace('sales_', '')
    modality_name = modality_name.replace('/', '')

    print(
        f'Building {modality_name} {start_week} - {end_week} in {write_mode} mode with stratum week of {stratum_week}')
    print()

    weights_filepath = config.embedded_dimensions_dir + config.segment_behavior_dir + "/weights"

    # preferred store
    preferred_div = (ingress.pull_new_vs_existing_mkts(end_week)
                     # don't need this column in this DF after filtering
                     .drop('pref_division'))

    # pull modality vintage and KPIs
    full_modality_vintage = ingress.pull_vintages_df(acds, modality_name, None, None, 'hh')
                            
    full_modality_kpis = ingress.pull_vintages_df(acds, modality_name, None, None, 'sales')

    new_hhs_df = segments.pull_new_hhs(full_modality_vintage, start_week, end_week)

    lapsed_hhs_df = segments.pull_lapsed_hhs(full_modality_kpis, start_week, end_week)

    quarters_df = dates.pull_quarters_for_year(acds, start_week, end_week)

    active_modality_kpis = (ingress.pull_vintages_df(acds, modality_name, start_week, end_week, 'sales')
                                .select('ehhn', 'fiscal_week', 'weekly_sales', 'weekly_visits', 'weekly_units'))

    # determine who is H/M/L eligible, and who is inactive
    active_hhs_df = segments.pull_active_hhs(active_modality_kpis, start_week, end_week, quarters_df)

    inconsistent_hhs_df = segments.pull_inconsistent_hhs(active_hhs_df, new_hhs_df)

    preassigned_hml_hhs_df = segments.pull_preassigned_hml_hhs(active_hhs_df, new_hhs_df)

    active_hml_kpis = hml.pull_hml_kpis(active_modality_kpis, preassigned_hml_hhs_df, quarters_df)

    active_ent_quarterly_behavior = hml.pull_ent_quarterly_behavior(acds, start_week, end_week, quarters_df)

    # calling it pillars because it has data for spend and unit penetration (both of the pillars towards overall funlo)
    pillars_df = (hml.combine_pillars(active_hml_kpis, active_ent_quarterly_behavior)
                  # left join because we want to include households without a preferred store
                  .join(preferred_div, 'ehhn', 'left')
                  # we assume they're part of an existing division unless they specifically have
                  # the encoding of a storeless market. Most of these households are lapsed
                  .fillna('existing', ['new_market_div']))

    qtr_weights_df = utils.create_quarter_weights_df(configs.DATE_RANGE_WEIGHTS)

    hml_weighted_df = hml.create_weighted_df(pillars_df, qtr_weights_df)

    column_order = ["ehhn", "modality", "end_week", "stratum_week", "quarter", "sales", "spend_percentile",
                    "spend_rank", "units", "enterprise_units", "unit_penetration", "penetration_percentile",
                    "penetration_rank", "quarter_points", "weight", "recency_adjusted_quarter_points"]

    print(weights_filepath)
    print(column_order)
    utils.write_to_delta(
        df_to_write=hml_weighted_df,
        filepath=weights_filepath,
        write_mode=write_mode,
        modality=modality_name,
        end_week=end_week,
        stratum_week=stratum_week,
        column_order=column_order
    )

    segment_filepath = configs.embedded_dimensions_dir + configs.segment_behavior_dir + "/segmentation"

    hml_weighted_df = \
        (spark.read.format("delta").load(weights_filepath).filter(f.col('modality') == modality_name).filter(f.col('stratum_week') == stratum_week))

    hml_hhs_df = hml.create_weighted_segs(hml_weighted_df)

    final_segs = segments.finalize_segs(new_hhs_df, lapsed_hhs_df, inconsistent_hhs_df.select('ehhn', 'segment'),
                                        hml_hhs_df)

    column_order = ["ehhn", "modality", "end_week", "stratum_week", "segment"]

    print(segment_filepath)
    print(column_order)
    utils.write_to_delta(
        df_to_write=final_segs,
        filepath=segment_filepath,
        write_mode=write_mode,
        modality=modality_name,
        end_week=end_week,
        stratum_week=stratum_week,
        column_order=column_order
    )   

# COMMAND ----------

# Accelerate queries with Delta: This query contains a highly selective filter. To improve the performance of queries,
# convert the table to Delta and run the OPTIMIZE ZORDER BY command on the table
# abfss:REDACTED_LOCAL_PART@sa8451posprd.dfs.core.windows.net/calendar/current. Learn more
