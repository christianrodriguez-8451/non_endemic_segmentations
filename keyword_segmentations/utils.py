"""This file contains various helper functions to automate the keyword-based segment creation process."""
from typing import List, Dict, Callable

from fuzzywuzzy import fuzz
from collections import Counter
from functools import reduce
import datetime as dt

from pyspark.sql import DataFrame, SparkSession, Column
import pyspark.sql.functions as f
import pyspark.sql.types as t

import flowcate

import config

def get_spark() -> SparkSession:
    """Attempt to grab the currently active ``SparkSession``.

    :return: The current ``SparkSession`` object
    :rtype: ``SparkSession``
    :raises ValueError: If there is no currently running session
    """
    spark = SparkSession._instantiatedSession  # `None` if no session found
    if spark is None:
        raise ValueError("Could not find a running Spark session")

    return spark


# Fuzzy matching
match_string = fuzz.token_sort_ratio
MatchUDf = f.udf(match_string, t.DoubleType())


# PIM
def get_most_recent_file(path: str) -> DataFrame:
    """Get the most recent file.

    This function takes a date-partitioned path as input and returns the most
    recent version.

    :param str path: The base path to search from.
    :return: The read in DataFrame
    :rtype: ``DataFrame``
    """
    # Get spark
    spark = get_spark()

    # Get the path
    fp = flowcate.files.FilePath(path)
    out_path = fp.walk_back(dt.datetime.now())

    return spark.read.parquet(out_path)


# Analysis
def multiway_keyphrase_matching(fields: List[str], keyphrases: List[str]) -> Column:
    return reduce(
        lambda a, b: a | b,
        [f.lower(f.col(col_name)).contains(phrase.lower())
         for col_name in fields
         for phrase in keyphrases]
    )


def multiway_exact_matching(field_dict: Dict[str, list]) -> Column:
    return reduce(
        lambda a, b: a | b,
        [f.col(col_name) == value
         for col_name, values in field_dict.items()
         for value in values]
    )


def display_product_hierarchy(df: DataFrame):
    for field in config.product_cols:
        if field.startswith('fyt_'):
            (
                df
                .groupBy(field)
                .count()
                .orderBy('count', ascending=False)
                .display()
            )



def extract_keywords(df: DataFrame,
                     fields: List[str],
                     strip_chars: str,
                     drop_mfr: bool,
                     mfr_col: str,
                     filters: List[Callable[str, bool]]
                     ) -> Dict[str, int]:
    # Extract fields
    all_fields = []
    for row in df.select(*fields, mfr_col).collect():
        mfr = getattr(row, mfr_col)
        mfr = mfr.lower().strip(strip_chars) if mfr is not None else ''

        for field in fields:
            val = getattr(row, field)
            if val is None:
                continue
            val = val.lower()

            if drop_mfr:
                val = val.replace(mfr, '')

            all_fields.append(val)

    # Replace with keywords
    all_keywords = [keyword
                    for field in all_fields
                    for word in field.split()
                    if len(keyword := word.strip().strip(strip_chars)) > 0
                    and all(f(keyword) for f in filters)]

    return Counter(all_keywords)