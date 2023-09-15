
from pyspark.sql import functions as f
from pyspark.sql import Window

########################
# PURPOSE
########################
##
# This file establishes which households fall into which segments. It splits into several groups:
##
# 1) lapsed - households who had positive modality visits more than 52 weeks ago, but not in the past 52 weeks
# 2) new - households whose first engagement (vintage) with the modality is in the past 52 weeks
# 3) inconsistent - households whose vintage was more than 52 weeks ago,
#     but have fewer than 7 visits or fewer than 3 distinct rolling quarters with visits in the past 52 weeks
# 4) HML - households who have more engagement than inconsistent. These are segmented in `hmls`
# 5) inactive - these households are identified and filtered out
##


def pull_new_hhs(vintage_hhs_df, start_week, end_week):
    """
    This function pulls households that are new to the modality (vintage in last 52 weeks).

    Inputs:
    1) vintage_hhs_df - spark DF, output of `pull_from_vintages.pull_vintages_df`. At household level
    2) start_week - str, first week to pull
    3) end_week - str, last week to pull (everything in between also pulled)

    Output:
    new_hhs_df - spark DF with 'ehhn', and 'segment' (value of "new") for columns
    """

    new_hhs_df = (vintage_hhs_df
                  .filter(f.col('vintage_week').between(start_week, end_week))
                  .select('ehhn')
                  .distinct()
                  .withColumn('segment', f.lit('new'))
                  )

    return new_hhs_df


def pull_lapsed_hhs(vintage_kpis_df, start_week, end_week):
    """
    This function pulls households that have lapsed from the modality (positive visits
         52+ weeks ago, 0 visits in the past 52).

    Inputs:
    1) vintage_kpis_df - spark DF, output of `pull_from_vintages.pull_vintages_df`. At KPI level
    2) start_week - str, first week to pull
    3) end_week - str, last week to pull (everything in between also pulled)

    Output:
    lapsed_hhs_df - spark DF with 'ehhn', and 'segment' (value of "lapsed") for columns
    """

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
    """
    This function pulls households that have modality visits in the past 52 weeks.

    NOTE: new households will be included in these groups. They must be anti-joined out

    Inputs:
    1) vintage_kpis_df - spark DF, output of `pull_from_vintages.pull_vintages_df`. At KPI level
    2) start_week - str, first week to pull
    3) end_week - str, last week to pull (everything in between also pulled)
    4) quarters_df - spark DF, maps a fiscal_week to its quarter

    Output:
    active_hhs_df - spark DF with 'ehhn', and 'segment' (value of "HML" or "inconsistent") for columns
    """

    # use this window to calculate total visits over quarters
    w = Window.partitionBy('ehhn')

    active_hhs_df = (vintage_kpis_df
                     # need this to calculate number of distinct rolling quarters visited
                     .join(quarters_df, 'fiscal_week', 'inner')

                     # don't want to include a quarter if it doesn't have positive sales (can rarely occur in vintages)
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
                                       (f.col('quarters') >= 3),
                                       f.lit('HML'))  # s.m. set this to 2 since noone met 3 threshold

                                 # if you have sales & visits but you don't meet HML threshold then you are inconsistent
                                 .otherwise('inconsistent')
                                 )

                     # explicitly filter these out
                     .filter(f.col('segment') != 'inactive')
                     )

    return active_hhs_df


def filter_active_hhs(active_hhs_df, segment_filter, anti_join_df=None):
    """
    This function filters the output from `pull_active_hhs` to the desired segment. It also enables
    anti-joining households out by another DF (used for removing new households)

    Inputs:
    1) active_hhs_df - spark DF, output from `pull_active_hhs`
    2) segment_filter - str, desired segment to filter to. In practice can only be "inconsistent" or "HML"
    3) anti_join_df - spark DF, defaults to None. Will anti_join on ehhn to `active_hhs_df` if not None
    """

    if segment_filter not in ['inactive', 'HML', 'inconsistent']:
        raise ValueError("`segment_filter` value must be from ['inactive', 'HML', 'inconsistent']")

    filtered_df = active_hhs_df.filter(f.col('segment') == segment_filter)

    if anti_join_df is not None:
        filtered_df = filtered_df.join(anti_join_df, 'ehhn', 'left_anti')

    return filtered_df


def pull_inconsistent_hhs(active_hhs_df, new_hhs_df):
    """Helpful wrapper around filter_active_hhs to identify inconsistent households"""
    return filter_active_hhs(active_hhs_df, 'inconsistent', new_hhs_df).select('ehhn', 'segment')


def pull_preassigned_hml_hhs(active_hhs_df, new_hhs_df):
    """Helpful wrapper around filter_active_hhs to identify HML households"""
    return filter_active_hhs(active_hhs_df, 'HML', new_hhs_df)


def finalize_segs(new_hhs_df,
                  lapsed_hhs_df,
                  inconsistent_hhs_df,
                  hml_hhs_df
                  ):
    """
    This function is the last in the segmentation process. Unions all segments together and returns them.
    """

    final_df = (lapsed_hhs_df
                .unionByName(new_hhs_df)
                .unionByName(inconsistent_hhs_df)
                .unionByName(hml_hhs_df)
                )

    return final_df
