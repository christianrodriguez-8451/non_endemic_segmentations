import pandas as pd
from effodata import ACDS

# spark
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

########################
# PURPOSE
########################
##
# This file contains utility functions that don't have a natural home in other areas.
##


def get_spark_session():
    """
    Finds the spark session if it exists, or raises an error if there is none.
    Found it in effodata and directly replicated
    """
    # took this from effodata
    spark = SparkSession._instantiatedSession  # type: ignore

    if spark is None:
        raise ValueError("no active SparkSession could be located")

    return spark


def create_quarter_weights_df(DATE_RANGE_WEIGHTS):
    """
    This function converts the date_range_weights dictionary (stored in config.json) into
    a pyspark DF that can be joined onto the quarter level data.

    Input:
    DATE_RANGE_WEIGHTS - dict, maps each quarter 1-4 to its weight (4 most recent / heaviest weight)

    Output:
    qtr_weights_df - spark DF with 4 rows, maps each quarter to its weight
    """
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
    """
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
    """
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
