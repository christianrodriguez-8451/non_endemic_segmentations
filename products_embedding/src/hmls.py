
from pyspark.sql import functions as f
from pyspark.sql import Window

import products_embedding.src.pull_input_data as ingress


########################
# PURPOSE
########################
##
# This file establishes assumes it is passed the correct households that need to be HML'd.
# It generates the spend and unit penetration pillars and assigns weights accordingly
##

def pull_hml_kpis(active_modality_kpis, preassigned_hml_hhs_df, quarters_df):
    """
    This function calculates quarter level spend and unit KPIs from provided Vintages DF.

    Inputs:
    1) active_modality_kpis - spark DF of vintage KPIs for desired modality
    2) preassigned_hml_hhs_df - spark DF of households that deserve an HML
    3) quarters_df - spark DF that maps fiscal_week to quarter

    Output:
    active_hml_kpis - spark DF that aggregates modality spend and units at ehhn-quarter level
    """

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
    """
    This function pulls enterprise visits so we can calculate modality visit penetration

    Inputs:
    1) acds - ACDS object, input to `pull_vintages_df`
    2) config - dict of configs, used for filepaths
    3) start_week - first fiscal_week to pull (oldest)
    4) end_week - last fiscal_week to pull (most recent)
    5) quarters_df - spark DF that maps fiscal_week to quarter

    Output:
    active_ent_quarterly_behavior - spark DF that aggregates modality visits at ehhn-quarter level
    """

    active_ent_quarterly_behavior = (ingress.pull_vintages_df(acds, 'enterprise', start_week, end_week, 'sales')
                                     .select('ehhn', 'fiscal_week', 'weekly_units')
                                     .join(quarters_df, 'fiscal_week', 'inner')
                                     .groupBy('ehhn', 'quarter')
                                     .agg(f.sum('weekly_units').alias('enterprise_units'))
                                     )

    return active_ent_quarterly_behavior


def combine_pillars(active_hml_kpis, active_ent_quarterly_behavior):
    """
    This function combines enterprise quarter visits with modality quarter aggregations.
    Produces modality visit penetration pillar necessary for weighting

    Inputs:
    1) active_hml_kpis - spark DF, output of `pull_hml_kpis`
    2) active_ent_quarterly_behavior - spark DF, output of `pull_ent_quarterly_behavior`

    Output:
    pillars_df - spark DF, contains all information needed to weight households on pillars
    """

    pillars_df = (active_hml_kpis
                  .join(active_ent_quarterly_behavior, ['ehhn', 'quarter'], 'inner')
                  .withColumn('unit_penetration',
                              f.round(f.col('units') / f.col('enterprise_units'), 4)
                              )
                  )

    return pillars_df


def create_weighted_df(pillars_df, qtr_weights_df, spend_thresholds=(.5, .9), visit_thresholds=(.5, .9)):
    """
    This function assigns weights at a quarter level based on provided cutoffs and calculated percentiles.
    Then it assigns initial HML groups at a quarter level, and uses them to assign quarter points.
    Last, it weights quarter points by quarter recency weights

    Inputs:
    1) pillars_df - spark DF, output from `combine_pillars`
    2) qtr_weights_df - spark DF, maps quarter number to its weight. output from `utils.create_quarter_weights_df`
    3) spend_thresholds - these can be custom specified as a tuple, but have sensible defaults
    3) visit_thresholds - these can be custom specified as a tuple, but have sensible defaults

    Output:

    """
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
    """
    This function is the final step in the HMLs process. It takes the output from `create_weighted_df` and
    sums the scores to get the final HML points for each household. This is used to then assign final
    H/M/L scores.

    Input:
    1) weighted_df - spark DF, output from `create_weighted_df`

    Output:
    weighted_segs - spark DF, has HMLs assigned to each household
    """
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
