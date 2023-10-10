# IMPORT PACKAGES
import resources.config as config
import pyspark.sql.functions as f
from pyspark.sql import types as t
from pyspark.sql.column import Column
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set, substring_index, concat_ws, concat, split, regexp_replace, size, expr, \
    when, array_distinct, collect_list
import pandas as pd
import numpy as np

pd.set_option('display.max_columns', 500)
from effodata import ACDS, golden_rules, Joiner, Sifter, Equality, sifter, join_on, joiner
from kpi_metrics import (
     KPI,
     AliasMetric,
     CustomMetric,
     AliasGroupby,
     Rollup,
     Cube,
     available_metrics,
     get_metrics
)

acds = ACDS(use_sample_mart=False)
kpi = KPI(use_sample_mart=False)

# These packages are required for delivery
from sentence_transformers import SentenceTransformer, util

# Specify the model directory on DBFS
model_dir = "/dbfs/dbfs/FileStore/users/s354840/pretrained_transformer_model"

# Loading the transformer model
model = SentenceTransformer(model_dir)

########################
# PURPOSE
########################
##
# This file contains utility functions that don't have a natural home in other areas.
##


@f.udf(returnType=t.FloatType())
def get_dot_product_udf(a: Column, b: Column):
    return float(np.asarray(a).dot(np.asarray(b)))


def create_upc_json(df, query):
    upc_list = df.rdd.map(lambda column: column.gtin_no).collect()
    upc_string = '","'.join(upc_list)

    upc_format = '{"cells":[{"order":0,"type":"BUYERS_OF_PRODUCT","purchasingPeriod":{"startDate":"' + iso_start_date + '","endDate":"' + iso_end_date + '","duration":52},"cellRefresh":"DYNAMIC","behaviorType":"BUYERS_OF_X","xProducts":{"upcs":["' + upc_string + '"]},"purchasingModality":"ALL"}],"name":"' + query + '","description":"Buyers of ' + query + ' products."}'
    return upc_format


def create_search_df(dot_products_df):
    search_output_df = (dot_products_df.filter(f.col('dot_product') >= .3))
    return search_output_df


def create_dot_df(product_vectors_df, array_query_col):
    dot_products_df = (
        product_vectors_df.withColumn('dot_product', get_dot_product_udf("vector", array_query_col)).drop(
            'vector').withColumn('dot_product_rank', f.rank().over(Window().orderBy(f.col('dot_product').desc()))))
    return dot_products_df


def create_array_query(query_vector):
    array_query_col = f.array([f.lit(i) for i in query_vector])
    return array_query_col


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


def pyspark_databricks_widget_check_for_empty_text_field(widget_name):
    """Checks if the given Databricks widget is empty.

    Args:
    widget_name: The name of the Databricks widget.

    Returns:
    True if the widget is empty, False otherwise.
    """

    widget_value = config.dbutils.widgets.get(widget_name)
    return widget_value == ""
