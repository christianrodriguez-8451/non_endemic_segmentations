# Databricks notebook source
#Define service principals
service_credential = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-pw')
service_application_id = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-app-id')
directory_id = "5f9dc6bd-f38a-454a-864c-c803691193c5"
storage_account = 'sa8451posprd'

# COMMAND ----------

#Set configurations
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

#Define service principals
service_credential = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-pw')
service_application_id = dbutils.secrets.get(scope='kv-8451-tm-media-dev',key='spTmMediaDev-app-id')
directory_id = "5f9dc6bd-f38a-454a-864c-c803691193c5"
storage_account = 'sa8451dbxadhocprd'

# COMMAND ----------

#Set configurations
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", service_application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

from effodata import ACDS, golden_rules
import kayday as kd

# this doesn't seem to work like get_spark_session... maybe refactor
acds = ACDS(use_sample_mart=False)

import pyspark.sql.functions as f

# get current datetime
from datetime import datetime
from dateutil.relativedelta import relativedelta

end_week_widget = ''
write_mode = 'overwrite'

def get_weeks_ago_from_fw(end_week = None, weeks = None):
    '''
    This function is used to calculated the week that is x weeks away from the provided start week.
    This is useful for example when you want to calculate the year ago week (take the current week and
    subtract 51 weeks). 

    Notes for use:
    - if both fields are None then you'll get the current fiscal_week 

    Inputs:
    1) end_week - str, defaults to None (which will use the current day to generate the end week)
    2) weeks - int, defaults to 0 (which will just return the end_week)

    Output:
    fw - str representing the desired fiscal_week
    '''
    if weeks is None:
        weeks = 0
    
    if end_week is None:
        # use today
        fw = kd.DateRange(f'{weeks} week ago').start_date.format_week()[1:]
        
    else:
        kd_end_week = kd.KrogerDate(year=int(end_week[:4]),
                                    period=int(end_week[4:6]),
                                    week=int(end_week[6:]), 
                                    day=1)
        
        fw = kd_end_week.range_ago(weeks=weeks).start_date.format_week()[1:]
        
    return fw

if end_week_widget == '':
    # this needs to be one week ahead of the last week to be included in the seg
    # makes sense for automation, but a little unintuitive for restatement
    # example: if we want to restate the seg for data ending 2022 P1W3, then the
    # current week value needs to be 2022 P1W4, because we will go backwards one week
    current_week = None

    # last week
    end_week = get_weeks_ago_from_fw(current_week, 1)
    # gives 52 weeks of history
    start_week = get_weeks_ago_from_fw(end_week, 51)
    # assign current week an actual value
    stratum_week = (kd.KrogerDate(year=int(end_week[:4]), 
                                  period=int(end_week[4:6]), 
                                  week=int(end_week[6:]), 
                                  day=1)
                    
                        # move ahead one week for stratum
                        .ahead(weeks=1)
                        .format_week()[1:] # remove the "F" at the beginning
                   )
elif len(end_week_widget):# == 8 and end_week_widget.isnumeric():
    # rough check that we have a valid fiscal week. KrogerDate will error if it isn't valid
    kd.KrogerDate(int(end_week_widget[:4]), int(end_week_widget[4:6]), int(end_week_widget[6:]), 1)

    # last week
    end_week = get_weeks_ago_from_fw(end_week_widget, 0)
    # gives 52 weeks of history
    start_week = get_weeks_ago_from_fw(end_week, 51)
    # assign current week an actual value
    stratum_week = (kd.KrogerDate(int(end_week[:4]), int(end_week[4:6]), int(end_week[6:]), 1)
                        .ahead(weeks=1)
                        .format_week()[1:]
                   )
else:
    raiseValueError("please input a valid fiscal week of the form YYYYPPWW")

DATE_RANGE_WEIGHTS = {4: 0.27, 3: 0.26, 2: 0.24, 1: 0.23}

embedded_dimensions_dir = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions'
vintages_dir = '/customer_data_assets/vintages'

# COMMAND ----------

import pandas as pd
########################
## PURPOSE
########################
##
## This file contains utility functions that don't have a natural home in other areas.
##
def get_spark_session():
    '''
    Finds the spark session if it exists, or raises an error if there is none. 
    Found it in effodata and directly replicated
    '''
    # took this from effodata
    spark = SparkSession._instantiatedSession  # type: ignore

    if spark is None:
        raise ValueError("no active SparkSession could be located")

    return spark

def create_quarter_weights_df(DATE_RANGE_WEIGHTS):
    '''
    This function converts the date_range_weights dictionary (stored in config.json) into
    a pyspark DF that can be joined onto the quarter level data.

    Input:
    DATE_RANGE_WEIGHTS - dict, maps each quarter 1-4 to its weight (4 most recent / heaviest weight)

    Output:
    qtr_weights_df - spark DF with 4 rows, maps each quarter to its weight
    '''
    spark = get_spark_session()
    
    keys_list, values_list = [], []

    for k in DATE_RANGE_WEIGHTS.keys():
        keys_list.append(k)
        values_list.append(DATE_RANGE_WEIGHTS.get(k))

    reformatted_dict = {'quarter': keys_list,
                        'weight': values_list
                       }

    qtr_weights_df = spark.createDataFrame(pd.DataFrame.from_dict(reformatted_dict))
    
    return qtr_weights_df
  
def write_to_delta(df_to_write, filepath, write_mode, modality, end_week, stratum_week, column_order):
  '''
  This function takes the provided inputs and writes the DF to delta. Mainly used to determine if we 
  overwrite or append data.

  Inputs:
  1) df_to_write - spark DF that we want to store
  2) filepath - the path to write to
  3) write_mode - one of either "append" or "overwrite"
  4) modality - the modality of this type of funlo
  5) end_week - the last week of data this run is generated over
  6) stratum_week - stratum prefers to lead by one week
  7) column_order - reorders columns for cleaner use when pulling

  Output:
  None (function doesn't return anything)
  '''
  if write_mode == 'append':
      (df_to_write
            # add the constant value columns
            # end week is the last data available in seg
            .withColumn('modality', f.lit(modality))
            .withColumn('end_week', f.lit(end_week))
            .withColumn('stratum_week', f.lit(stratum_week))

            .select(column_order)

            .write
            .format('delta')
            .mode(write_mode)
            .partitionBy('modality', 'stratum_week')
            .save(filepath)
      )    

  elif write_mode == 'overwrite':
      (df_to_write
            # add the constant value columns
            # end week is the last data available in seg
            .withColumn('modality', f.lit(modality))
            .withColumn('end_week', f.lit(end_week))
            .withColumn('stratum_week', f.lit(stratum_week))

            .select(column_order)

            .write
            .format('delta')
            .mode('overwrite')
            .option("partitionOverwriteMode", "dynamic")
            .partitionBy('modality', 'stratum_week')
            .save(filepath)
      )
  else:
      raise ValueError("acceptable `write_mode` values are 'append' and 'overwrite'")

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import Window

########################
## PURPOSE
########################
##
## This file is used for all date calculations and applications
##
def pull_from_acds_dates(fw_start, fw_end, additional_cols=None):
    '''
    This function pulls fiscal_week and any other desired columns from ACDS.dates

    Inputs:
    1) acds - ACDS object
    2) fw_start - str, first week to pull
    3) fw_end - str, last week to pull (everything in between also pulled)
    4) additional_cols - list of strs, optional

    Output:
    dates_df - distinct spark DF with fiscal_week on provided dates and any other desired columns
    '''
    
    if additional_cols is None:
        select_cols = ['fiscal_week']
    else:
        select_cols = ['fiscal_week'] + additional_cols
    
    dates_df = (acds.dates
         .select(select_cols)
         .distinct()
         .filter(f.col('fiscal_week').between(fw_start, fw_end))
    )
    
    return dates_df

def convert_fw_start_and_end_to_list(fw_start, fw_end):
    '''
    This function uses `pull_from_acds_dates` to generate the desired list of fiscal_weeks in ascending order.

    Inputs:
    1) fw_start - str, first week to pull
    2) fw_end - str, last week to pull (everything in between is also pulled)

    Output:
    fw_list - list of strs, useful for filtering or looping over
    '''

    # uses `.toPandas()` because it's supposed to be more efficient than collecting
    fw_list = list(pull_from_acds_dates(fw_start, fw_end).orderBy('fiscal_week').toPandas()['fiscal_week'])
    return fw_list

def pull_quarters_for_year(acds, fw_start, fw_end):
    '''This function uses `pull_from_acds_dates` to generate a list of fiscal weeks
    over the past year and assigns them quarters. Necessary for HML process

    Inputs:
    1) acds - ACDS object
    2) fw_start - fw_start - str, first week to pull
    3) fw_end - str, last week to pull (everything in between is also pulled)
    '''
    
    w = Window.partitionBy().orderBy(f.col('fiscal_week'))

    # quarter 1 is oldest, quarter 4 is most recent
    quarters_df = (pull_from_acds_dates(fw_start, fw_end)
         .withColumn('row_num', f.row_number().over(w))

         # use ceil int divided by 13 to create just 4 groups
         .withColumn('quarter', f.ceil(f.col('row_num') / 13))
         .drop('row_num')
         .orderBy('fiscal_week')
    )
    
    return quarters_df

# COMMAND ----------

# spark
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as f

# internal


########################
## PURPOSE
########################
##
## This file handles all data imports (Vintages and preferred Store)
##

def pull_vintages_df(acds, 
                     modality, 
                     start_fw = None, 
                     end_fw = None, 
                     vintage_type = 'sales'):
    '''
    This function uses the filepaths stored in the config file and pulls data from Vintages.
    
    Inputs:
    1) acds - ACDS object (see if this can be removed... used to generate list of weeks but now that's in `convert_fw_start_and_end_to_list`)
    2) config - dictionary of config information. Read from json at start of process
    3) modality - 

    Output:
    `vintage_df` - spark DF of requested modality on requested time at either HH or KPI level
    '''
    
    #if modality not in ["ketogenic", "enterprise", "vegan", "paleo", "vegetarian"]:
    #    raise ValueError(f'`modality` must be one of "ketogenic", "vegan", "paleo" or "enterprise".')
    
    if vintage_type == 'sales':
        filter_col = 'fiscal_week'
    elif vintage_type == 'hh':
        filter_col = 'vintage_week'
    else:
        raise ValueError(f'vintage_type must be either "sales" for VintageKPI data, or "hh" for vintage week')
        
    # defaults to None (pull all history) and is overwritten if valid start and end weeks are supplied
    fws_list = None
        
    if start_fw is None and end_fw is None:
        # 1. handles the case we want to pull ALL history
        fws_to_pull = '*'
    elif start_fw is not None and end_fw is None:
        # 2. we want to pull a specific week
        fws_to_pull = start_fw
    elif start_fw is not None and end_fw is not None:
        # 3. pull all history, then filter to the speciifc weeks generated
        fws_to_pull = '*'
        fws_list = convert_fw_start_and_end_to_list(start_fw, end_fw)
    else:
        fw_error = '''
        There are 3 acceptable configurations for `start_fw` and `end_fw` parameters:
        1. both set to None to pull all fiscal weeks
        2. just end_fw set to None to pull just the start fw
        3. neither set to None to pull all fiscal weeks from start to end (inclusive)
        '''
        raise ValueError(fw_error)
        
    spark = get_spark_session()
    
    # use config to store file paths
    base_path = embedded_dimensions_dir + vintages_dir
    config_vintage_modality = '/' + vintage_type + '_' + modality
    full_parquet = base_path + config_vintage_modality + '/' + filter_col + '=' + fws_to_pull

    if fws_list is not None:
        # case 3 above (only one that we filter many weeks to a subset)
        vintages_df = (
            spark.read.option('basePath', f'{base_path}')
                 .parquet(full_parquet)
                 .filter(f.col(filter_col).isin(fws_list))
        )
    else:
        # case 1 and 2
        vintages_df = spark.read.option('basePath', f'{base_path}').parquet(full_parquet)
    
    return vintages_df

def pull_new_vs_existing_mkts(end_week):

    spark = get_spark_session()
    
    new_mkts_df = (spark.read.parquet("abfss://acds@sa8451posprd.dfs.core.windows.net/preferred_store")
                              .filter(f.col('fiscal_week') == end_week)
                              .withColumn('new_market_div', 
                                              f.when((f.substring(f.col('pref_division'), 1, 3).isin(['540', '541', '542'])) &
                                                     (f.length('pref_division') > 3), 'new market')
                                               .otherwise('existing')
                                         )
                              .select('ehhn', 'pref_division', 'new_market_div')
    )
    
    return new_mkts_df

# COMMAND ----------

########################
## PURPOSE
########################
##
## This file establishes which households fall into which segments. It splits into several groups:
## 
## 1) lapsed - households who had positive modality visits more than 52 weeks ago, but not in the past 52 weeks
## 2) new - households whose first engagement (vintage) with the modality is in the past 52 weeks
## 3) inconsistent - households whose vintage was more than 52 weeks ago, 
##     but have fewer than 7 visits or fewer than 3 distinct rolling quarters with visits in the past 52 weeks
## 4) HML - households who have more engagement than inconsistent. These are segmented in `hmls`
## 5) inactive - these households are identified and filtered out
##

def pull_new_hhs(vintage_hhs_df, start_week, end_week):
    '''
    This function pulls households that are new to the modality (vintage in last 52 weeks).
    
    Inputs:
    1) vintage_hhs_df - spark DF, output of `pull_from_vintages.pull_vintages_df`. At household level
    2) start_week - str, first week to pull
    3) end_week - str, last week to pull (everything in between also pulled)

    Output:
    new_hhs_df - spark DF with 'ehhn', and 'segment' (value of "new") for columns
    '''
    
    new_hhs_df = (vintage_hhs_df
                  .filter(f.col('vintage_week').between(start_week, end_week))
                  .select('ehhn')
                  .distinct()
                  .withColumn('segment', f.lit('new'))
          )
    
    return new_hhs_df

def pull_lapsed_hhs(vintage_kpis_df, start_week, end_week):
    '''
    This function pulls households that have lapsed from the modality (positive visits
         52+ weeks ago, 0 visits in the past 52).

    Inputs:
    1) vintage_kpis_df - spark DF, output of `pull_from_vintages.pull_vintages_df`. At KPI level
    2) start_week - str, first week to pull
    3) end_week - str, last week to pull (everything in between also pulled)

    Output:
    lapsed_hhs_df - spark DF with 'ehhn', and 'segment' (value of "lapsed") for columns
    '''
    
    lapsed_hhs_df = (vintage_kpis_df
                     
                     # need this filter so we don't include future behavior from perspective of this run (restatements)
                     .filter(f.col('fiscal_week') <= end_week)
                     
                     .withColumn('time_period', 
                                 f.when(f.col('fiscal_week').between(start_week, end_week), f.lit('active_period'))
                                  # this otherwise is inclusive of future without above filter
                                  .otherwise(f.lit('lapsing_period'))
                                )
                     
                     .groupBy('ehhn')
                     .pivot('time_period', ['active_period', 'lapsing_period'])
                     .agg(f.sum('weekly_visits').alias('visits'))
                 
                     .fillna(0, ['active_period', 'lapsing_period'])
                     .filter(f.col('lapsing_period') > 0)
                     .filter(f.col('active_period') == 0)
                 
                     .select('ehhn')
                     .withColumn('segment', f.lit('lapsed'))
                )
    
    return lapsed_hhs_df

def pull_active_hhs(vintage_kpis_df, start_week, end_week, quarters_df):
    '''
    This function pulls households that have modality visits in the past 52 weeks.

    NOTE: new households will be included in these groups. They must be anti-joined out

    Inputs:
    1) vintage_kpis_df - spark DF, output of `pull_from_vintages.pull_vintages_df`. At KPI level
    2) start_week - str, first week to pull
    3) end_week - str, last week to pull (everything in between also pulled)
    4) quarters_df - spark DF, maps a fiscal_week to its quarter

    Output:
    active_hhs_df - spark DF with 'ehhn', and 'segment' (value of "HML" or "inconsistent") for columns
    '''

    # use this window to calculate total visits over quarters
    w = Window.partitionBy('ehhn')
    
    active_hhs_df = (vintage_kpis_df
                            # need this to calculate number of distinct rolling quarters visited
                            .join(quarters_df, 'fiscal_week', 'inner')

                            # we don't want to include a quarter if it doesn't have positive sales (can rarely occur in vintages)
                            .filter(f.col('weekly_sales') > 0)
                        
                            .groupBy('ehhn')
                            .agg(f.sum('weekly_sales').alias('sales'),
                                 f.sum('weekly_visits').alias('visits'),
                                 f.countDistinct('quarter').alias('quarters')
                                )
                        
                            .withColumn('total_visits', f.sum('visits').over(w))
                        
                             # in the past we used a threshold of 3 quarters for funlo loyal
                             # and 4 quarters for non-loyals. I'm interested in reducing to 3 
                             # across the board since we now require at least a year from vintage...
                            .withColumn('segment',
                                        # sales filter above should exclude inactive, but making it explicit
                                        f.when(f.col('total_visits') <= 0, f.lit('inactive')) 

                                        # both requirements must be met to get an HML 
                                        # this prevents rapidly changing cutoffs which cause instability
                                         .when((f.col('total_visits') >= 7) &
                                               (f.col('quarters') >= 3), f.lit('HML')) #s.m. set this to 2 since noone met 3 threshold

                                        # if you have sales and visits but you don't meet HML threshold then you are inconsistent
                                         .otherwise('inconsistent')
                                       )
                        
                            # explicitly filter these out
                            .filter(f.col('segment') != 'inactive')
                       )

    return active_hhs_df

def filter_active_hhs(active_hhs_df, segment_filter, anti_join_df = None):
    '''
    This function filters the output from `pull_active_hhs` to the desired segment. It also enables 
    anti-joining households out by another DF (used for removing new households)

    Inputs:
    1) active_hhs_df - spark DF, output from `pull_active_hhs`
    2) segment_filter - str, desired segment to filter to. In practice can only be "inconsistent" or "HML"
    3) anti_join_df - spark DF, defaults to None. Will anti_join on ehhn to `active_hhs_df` if not None
    '''
    
    if segment_filter not in ['inactive', 'HML', 'inconsistent']:
        raise ValueError("`segment_filter` value must be from ['inactive', 'HML', 'inconsistent']")
        
    filtered_df = active_hhs_df.filter(f.col('segment') == segment_filter)
    
    if anti_join_df is not None:
        filtered_df = filtered_df.join(anti_join_df, 'ehhn', 'left_anti')
    
    return filtered_df

def pull_inconsistent_hhs(active_hhs_df, new_hhs_df):
    '''Helpful wrapper around filter_active_hhs to identify inconsistent households'''
    return filter_active_hhs(active_hhs_df, 'inconsistent', new_hhs_df).select('ehhn', 'segment') 
  
def pull_preassigned_hml_hhs(active_hhs_df, new_hhs_df):
    '''Helpful wrapper around filter_active_hhs to identify HML households'''
    return filter_active_hhs(active_hhs_df, 'HML', new_hhs_df)
  
def finalize_segs(new_hhs_df, 
                  lapsed_hhs_df, 
                  inconsistent_hhs_df, 
                  hml_hhs_df
                 ):
    '''
    This function is the last in the segmentation process. Unions all segments together and returns them.
    '''
    
    final_df = (lapsed_hhs_df
                       .unionByName(new_hhs_df)
                       .unionByName(inconsistent_hhs_df)
                       .unionByName(hml_hhs_df)
               )
    
    return final_df

# COMMAND ----------

########################
## PURPOSE
########################
##
## This file establishes assumes it is passed the correct households that need to be HML'd.
## It generates the spend and unit penetration pillars and assigns weights accordingly
##

def pull_hml_kpis(active_modality_kpis, preassigned_hml_hhs_df, quarters_df):
    '''
    This function calculates quarter level spend and unit KPIs from provided Vintages DF.

    Inputs:
    1) active_modality_kpis - spark DF of vintage KPIs for desired modality
    2) preassigned_hml_hhs_df - spark DF of households that deserve an HML
    3) quarters_df - spark DF that maps fiscal_week to quarter

    Output:
    active_hml_kpis - spark DF that aggregates modality spend and units at ehhn-quarter level
    '''
    
    active_hml_kpis = (active_modality_kpis
                       # filter to HML households
                       .join(preassigned_hml_hhs_df.select('ehhn'), ['ehhn'], 'inner')
                       .join(quarters_df, 'fiscal_week', 'inner')
                   
                       # use these to generate quartiles
                       .groupBy('ehhn', 'quarter')
                       .agg(f.sum('weekly_sales').alias('sales'),
                            f.sum('weekly_units').alias('units')
                           )
                  )
    
    return active_hml_kpis

def pull_ent_quarterly_behavior(acds, start_week, end_week, quarters_df):
    '''
    This function pulls enterprise visits so we can calculate modality visit penetration

    Inputs:
    1) acds - ACDS object, input to `pull_vintages_df`
    2) config - dict of configs, used for filepaths
    3) start_week - first fiscal_week to pull (oldest)
    4) end_week - last fiscal_week to pull (most recent)
    5) quarters_df - spark DF that maps fiscal_week to quarter

    Output:
    active_ent_quarterly_behavior - spark DF that aggregates modality visits at ehhn-quarter level
    '''

    active_ent_quarterly_behavior = (pull_vintages_df(acds, 'enterprise', start_week, end_week, 'sales')
                                         .select('ehhn', 'fiscal_week', 'weekly_units')
                                         .join(quarters_df, 'fiscal_week', 'inner')
                                         .groupBy('ehhn', 'quarter')
                                         .agg(f.sum('weekly_units').alias('enterprise_units'))
                                    )
    
    return active_ent_quarterly_behavior

def combine_pillars(active_hml_kpis, active_ent_quarterly_behavior):
    '''
    This function combines enterprise quarter visits with modality quarter aggregations.
    Produces modality visit penetration pillar necessary for weighting

    Inputs:
    1) active_hml_kpis - spark DF, output of `pull_hml_kpis`
    2) active_ent_quarterly_behavior - spark DF, output of `pull_ent_quarterly_behavior`

    Output:
    pillars_df - spark DF, contains all information needed to weight households on pillars
    '''
    
    pillars_df = (active_hml_kpis
                  .join(active_ent_quarterly_behavior, ['ehhn', 'quarter'], 'inner')
                  .withColumn('unit_penetration', 
                              f.round(f.col('units') / f.col('enterprise_units'), 4)
                             )
             )
    
    return pillars_df

def create_weighted_df(pillars_df, qtr_weights_df, spend_thresholds = (.5, .9), visit_thresholds = (.5, .9)):
    '''
    This function assigns weights at a quarter level based on provided cutoffs and calculated percentiles.
    Then it assigns initial HML groups at a quarter level, and uses them to assign quarter points.
    Last, it weights quarter points by quarter recency weights

    Inputs:
    1) pillars_df - spark DF, output from `combine_pillars`
    2) qtr_weights_df - spark DF, maps quarter number to its weight. output from `utils.create_quarter_weights_df`
    3) spend_thresholds - these can be custom specified as a tuple, but have sensible defaults
    3) visit_thresholds - these can be custom specified as a tuple, but have sensible defaults

    Output:
    
    '''
    high_spend_cutoff = spend_thresholds[1]
    medium_spend_cutoff = spend_thresholds[0]
    
    high_visits_cutoff = visit_thresholds[1]
    medium_visits_cutoff = visit_thresholds[0]
    
    spend_window = Window.partitionBy('quarter').orderBy('sales')
    penetration_window = Window.partitionBy('quarter').orderBy('unit_penetration')
    
    weighted_df = (pillars_df
                       .withColumn(f'spend_percentile', f.percent_rank().over(spend_window))
                       .withColumn(f'penetration_percentile', 
                                   f.when(f.col(f'unit_penetration') == 1, 1)
                                    .otherwise(f.percent_rank().over(penetration_window))
                                  )
                   
                       .withColumn('spend_rank', 
                                   f.when(f.col('spend_percentile') >= high_spend_cutoff, 'H')
                                    .when(f.col('spend_percentile') >= medium_spend_cutoff, 'M')
                                    .otherwise('L')
                                  )
                       .withColumn('penetration_rank', 
                                   f.when(f.col('penetration_percentile') >= high_visits_cutoff, 'H')
                                    .when(f.col('penetration_percentile') >= medium_visits_cutoff, 'M')
                                    .otherwise('L')
                                  )
                   
                       .withColumn('quarter_points',
                                   
                                   # these 3 cover new markets
                                   f.when((f.col('spend_rank') == 'H') & (f.col('new_market_div') == 'new market'), 3)
                                    .when((f.col('spend_rank') == 'M') & (f.col('new_market_div') == 'new market'), 2)
                                    .when((f.col('spend_rank') == 'L') & (f.col('new_market_div') == 'new market'), 1)
                                   
                                   # these cover all existing markets which have BOTH pillars evaluated
                                    .when((f.col('spend_rank') == 'H') & (f.col('penetration_rank') == 'H'), 3)
                                    .when((f.col('spend_rank') == 'M') & (f.col('penetration_rank') == 'H'), 3)
                                    .when((f.col('spend_rank') == 'H') & (f.col('penetration_rank') == 'M'), 3)
                                   
                                    .when((f.col('spend_rank') == 'M') & (f.col('penetration_rank') == 'M'), 2)
                                    .when((f.col('spend_rank') == 'L') & (f.col('penetration_rank') == 'H'), 2)
                                    .when((f.col('spend_rank') == 'H') & (f.col('penetration_rank') == 'L'), 2)
                                   
                                    .when((f.col('spend_rank') == 'L') & (f.col('penetration_rank') == 'L'), 1)
                                    .when((f.col('spend_rank') == 'M') & (f.col('penetration_rank') == 'L'), 1)
                                    .when((f.col('spend_rank') == 'L') & (f.col('penetration_rank') == 'M'), 1)
                                    .otherwise(0)
                                  )
                   
                       .join(qtr_weights_df, 'quarter', 'left')
                   
                       .withColumn('recency_adjusted_quarter_points', 
                                   f.round(f.col('quarter_points') * f.col('weight'), 4)
                                  )
                  )
    
    return weighted_df
  
def create_weighted_segs(weighted_df):
    '''
    This function is the final step in the HMLs process. It takes the output from `create_weighted_df` and 
    sums the scores to get the final HML points for each household. This is used to then assign final 
    H/M/L scores.

    Input:
    1) weighted_df - spark DF, output from `create_weighted_df`

    Output:
    weighted_segs - spark DF, has HMLs assigned to each household
    '''
    weighted_segs = (weighted_df
                         .groupBy('ehhn')
                         .agg(f.sum('recency_adjusted_quarter_points').alias('weighted_points'))
                         .withColumn('segment',
                                     f.when(f.col('weighted_points') > 2, 'H')
                                      .when(f.col('weighted_points') > 1, 'M')
                                      .when(f.col('weighted_points') <= 1, 'L')
                                      .otherwise('ERROR')
                                    )
                        # selects these two columns to match other DFs for easy union
                         .select('ehhn', 'segment')
                    )
    
    return weighted_segs

# COMMAND ----------

diet_query_vintages_directories = spark.createDataFrame(list(dbutils.fs.ls(embedded_dimensions_dir + vintages_dir)))
diet_query_vintages_directories = diet_query_vintages_directories.filter(diet_query_vintages_directories.name.like('sales_%'))
diet_query_vintages_directories = diet_query_vintages_directories.rdd.map(lambda column: column.name).collect()
modality_list_nonship = diet_query_vintages_directories
#modality_list_nonship = ['ketogenic', 'paleo', 'vegan', 'vegetarian']
#spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

for modality_name in modality_list_nonship:
    modality_name = modality_name.replace('sales_', '')
    modality_name = modality_name.replace('/', '')
    segment_behavior_dir = '/customer_data_assets/segment_behavior'

    weights_filepath = embedded_dimensions_dir + segment_behavior_dir + "/weights"


    # preferred store
    preferred_div = (pull_new_vs_existing_mkts(end_week)
                        # don't need this column in this DF after filtering
                        .drop('pref_division')
                    )


    # pull modality vintage and KPIs
    full_modality_vintage = pull_vintages_df(acds, modality_name, None, None, 'hh')
                            
    full_modality_kpis = pull_vintages_df(acds, modality_name, None, None, 'sales') 

    new_hhs_df = pull_new_hhs(full_modality_vintage, start_week, end_week)

    lapsed_hhs_df = pull_lapsed_hhs(full_modality_kpis, start_week, end_week)

    quarters_df = pull_quarters_for_year(acds, start_week, end_week)

    active_modality_kpis = (pull_vintages_df(acds, modality_name, start_week, end_week, 'sales')
                                .select('ehhn', 'fiscal_week', 'weekly_sales', 'weekly_visits', 'weekly_units')
                          )
    # determine who is H/M/L eligible, and who is inactive
    active_hhs_df = pull_active_hhs(active_modality_kpis, start_week, end_week, quarters_df)

    inconsistent_hhs_df = pull_inconsistent_hhs(active_hhs_df, new_hhs_df)

    preassigned_hml_hhs_df = pull_preassigned_hml_hhs(active_hhs_df, new_hhs_df)

    active_hml_kpis = pull_hml_kpis(active_modality_kpis, preassigned_hml_hhs_df, quarters_df) #i will need to make this diet active HH

    active_ent_quarterly_behavior = pull_ent_quarterly_behavior(acds, start_week, end_week, quarters_df)

    # calling it pillars because it has data for spend and unit penetration (both of the pillars towards overall funlo)
    pillars_df = (combine_pillars(active_hml_kpis, active_ent_quarterly_behavior)
                      # left join because we want to include households without a preferred store
                      # we've already excluded bad divisions above (e.g. harris teeter - "097")
                      .join(preferred_div, 'ehhn', 'left')
                      # we assume they're part of an existing division unless they specifically have
                      # the encoding of a storeless market. Most of these households are lapsed
                      .fillna('existing', ['new_market_div'])
                )

    qtr_weights_df = create_quarter_weights_df(DATE_RANGE_WEIGHTS)

    hml_weighted_df = create_weighted_df(pillars_df, qtr_weights_df)

    column_order = ["ehhn", "modality", "end_week", "stratum_week", "quarter", 
                                  "sales", "spend_percentile", "spend_rank",
                                  "units", "enterprise_units", "unit_penetration", "penetration_percentile", "penetration_rank", 
                                  "quarter_points", "weight", "recency_adjusted_quarter_points"]

    print(weights_filepath)
    print(column_order)
    write_to_delta(
        df_to_write = hml_weighted_df,
        filepath = weights_filepath,
        write_mode = write_mode,
        modality = modality_name,
        end_week = end_week,
        stratum_week = stratum_week,
        column_order = column_order
    )

    segment_filepath = embedded_dimensions_dir + segment_behavior_dir + "/segmentation"

    hml_weighted_df = (spark.read.format("delta").load(weights_filepath)
                      .filter(f.col('modality') == modality_name)
                      .filter(f.col('stratum_week') == stratum_week)
                      ) 

    hml_hhs_df = create_weighted_segs(hml_weighted_df)

    final_segs = finalize_segs(new_hhs_df, 
                              lapsed_hhs_df, 
                              inconsistent_hhs_df.select('ehhn', 'segment'), 
                              hml_hhs_df)

    column_order = ["ehhn", "modality", "end_week", "stratum_week", "segment"]

    print(segment_filepath)
    print(column_order)
    write_to_delta(
        df_to_write = final_segs,
        filepath = segment_filepath,
        write_mode = write_mode,
        modality = modality_name,
        end_week = end_week,
        stratum_week = stratum_week,
        column_order = column_order
    )   

# COMMAND ----------

#Accelerate queries with Delta: This query contains a highly selective filter. To improve the performance of queries, convert the table to Delta and run the OPTIMIZE ZORDER BY command on the table abfss:REDACTED_LOCAL_PART@sa8451posprd.dfs.core.windows.net/calendar/current. Learn more
