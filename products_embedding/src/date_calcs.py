
from pyspark.sql import functions as f
from pyspark.sql import Window

import kayday as kd
from effodata import ACDS

# get current datetime
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

# internal
import products_embedding.src.utils as utils

########################
# PURPOSE
########################
##
# This file is used for all date calculations and applications
##

# minus 1 year
today_year_ago = (datetime.today() - relativedelta(years=1)).strftime('%Y%m%d')
today = datetime.today().strftime('%Y%m%d')
today_time = datetime.now()
duration_time = (today_time + relativedelta(weeks=52))
yesterday = date.today() - timedelta(days=1)
# Get current ISO 8601 datetime in string format
iso_start_date = today_time.isoformat()
iso_end_date = duration_time.isoformat()
dates_tbl = utils.acds.dates.select('trn_dt', 'fiscal_week', 'fiscal_month', 'fiscal_quarter', 'fiscal_year')

def pull_from_acds_dates(fw_start, fw_end, additional_cols=None):
    """
    This function pulls fiscal_week and any other desired columns from ACDS.dates

    Inputs:
    1) acds - ACDS object
    2) fw_start - str, first week to pull
    3) fw_end - str, last week to pull (everything in between also pulled)
    4) additional_cols - list of strs, optional

    Output:
    dates_df - distinct spark DF with fiscal_week on provided dates and any other desired columns
    """

    if additional_cols is None:
        select_cols = ['fiscal_week']
    else:
        select_cols = ['fiscal_week'] + additional_cols

    dates_df = (utils.acds.dates
                .select(select_cols)
                .distinct()
                .filter(f.col('fiscal_week').between(fw_start, fw_end))
                )

    return dates_df


def convert_fw_start_and_end_to_list(fw_start, fw_end):
    """
    This function uses `pull_from_acds_dates` to generate the desired list of fiscal_weeks in ascending order.

    Inputs:
    1) fw_start - str, first week to pull
    2) fw_end - str, last week to pull (everything in between is also pulled)

    Output:
    fw_list - list of strs, useful for filtering or looping over
    """

    # uses `.toPandas()` because it's supposed to be more efficient than collecting
    fw_list = list(pull_from_acds_dates(fw_start, fw_end).orderBy('fiscal_week').toPandas()['fiscal_week'])
    return fw_list


def get_weeks_ago_from_fw(end_week=None, weeks=None):
    """
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
    """
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


def get_one_week_ago():
    """helpful wrapper around `get_weeks_ago_from_fw`"""
    return get_weeks_ago_from_fw(None, 1)


def pull_quarters_for_year(acds, fw_start, fw_end):
    """This function uses `pull_from_acds_dates` to generate a list of fiscal weeks
    over the past year and assigns them quarters. Necessary for HML process

    Inputs:
    1) acds - ACDS object
    2) fw_start - fw_start - str, first week to pull
    3) fw_end - str, last week to pull (everything in between is also pulled)
    """

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


def get_fw(weeks_ago):
    today = kd.KrogerDate(date='today')
    weeks_ago_today = today.ago(weeks=weeks_ago)
    weeks_ago_today_week = weeks_ago_today.format_week()[1:]
    return weeks_ago_today_week


def get_start_date(weeks_ago):
    today = kd.KrogerDate(date='today')
    weeks_ago_today = today.ago(weeks=weeks_ago)
    weeks_ago_today_week = kd.DateRange(weeks_ago_today.format_week())
    week_start_date = weeks_ago_today_week.start_date.format_cal_date()
    return week_start_date


def get_end_date(weeks_ago):
    today = kd.KrogerDate(date='today')
    weeks_ago_today = today.ago(weeks=weeks_ago)
    weeks_ago_today_week = kd.DateRange(weeks_ago_today.format_week())
    week_end_date = weeks_ago_today_week.end_date.format_cal_date()
    return week_end_date
