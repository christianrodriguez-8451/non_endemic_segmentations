# spark
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as f

# internal
import products_embedding.src.utils as utils
import products_embedding.src.date_calcs as dates
import products_embedding.config as configs


########################
# PURPOSE
########################
##
# This file handles all data imports (Vintages and preferred Store)
##

def pull_vintages_df(acds,
                     modality,
                     start_fw=None,
                     end_fw=None,
                     vintage_type='sales'):
    """
    This function uses the filepaths stored in the config file and pulls data from Vintages.

    Inputs:
    1) acds - ACDS object (see if this can be removed... used to generate list of weeks but now that's in `convert_fw_start_and_end_to_list`)
    2) config - dictionary of config information. Read from json at start of process
    3) modality -

    Output:
    `vintage_df` - spark DF of requested modality on requested time at either HH or KPI level
    """

    # if modality not in ["ketogenic", "enterprise", "vegan", "paleo", "vegetarian"]:
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
        # 3. pull all history, then filter to the specific weeks generated
        fws_to_pull = '*'
        fws_list = dates.convert_fw_start_and_end_to_list(start_fw, end_fw)
    else:
        fw_error = '''
        There are 3 acceptable configurations for `start_fw` and `end_fw` parameters:
        1. both set to None to pull all fiscal weeks
        2. just end_fw set to None to pull just the start fw
        3. neither set to None to pull all fiscal weeks from start to end (inclusive)
        '''
        raise ValueError(fw_error)

    spark = utils.get_spark_session()

    # use config to store file paths
    base_path = configs.embedded_dimensions_dir + configs.vintages_dir
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
    spark = utils.get_spark_session()

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
