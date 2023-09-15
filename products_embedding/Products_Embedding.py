# Databricks notebook source
# Import service principal definitions functions, date calculation and hard coded configs from config
import resources.config as config
import products_embedding.src.date_calcs as dates

# IMPORT PACKAGES
from pyspark.sql import functions as f
from pyspark.sql.functions import collect_set, substring_index, concat_ws, concat, split, regexp_replace, size, expr, \
    when, array_distinct
import pandas as pd
pd.set_option('display.max_columns',500)
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

# These packages are required for delivery
from sentence_transformers import SentenceTransformer, util

# COMMAND ----------

acds = ACDS(use_sample_mart=False)
kpi = KPI(use_sample_mart=False)

# Set configurations
for sa in config.storage_account:
    spark.conf.set(f"fs.azure.account.auth.type.{sa}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{sa}.dfs.core.windows.net",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{sa}.dfs.core.windows.net", config.service_application_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{sa}.dfs.core.windows.net", config.service_credential)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{sa}.dfs.core.windows.net",
                   f"https://login.microsoftonline.com/{config.directory_id}/oauth2/token")

# COMMAND ----------

# Look at acds transactions for all households the past year
acds_previous_year_transactions = \
  acds.get_transactions(dates.today_year_ago,
                        dates.today, apply_golden_rules=golden_rules(['customer_exclusions', "fuel_exclusions"]))
acds_previous_year_transactions = acds_previous_year_transactions.where(acds_previous_year_transactions.scn_unt_qy > 0)
pyear_purchased_upcs = acds_previous_year_transactions.select(acds_previous_year_transactions.gtin_no).distinct()
pyear_purchased_upcs = pyear_purchased_upcs.select(pyear_purchased_upcs.gtin_no.alias('upc'))

pinto_path = \
  config.get_latest_modified_directory(config.azure_pinto_path)
pinto_prods = spark.read.parquet(pinto_path)
pim_path = \
  config.get_latest_modified_directory(config.azure_pim_core_by_cycle)
pim_core = spark.read.parquet(pim_path)

# COMMAND ----------

# Build a diet sentence with description and diet related meta data from PIM data source
pim_diet = pim_core.select(f.col("upc_key"), f.col("krogerOwnedEcommerceDescription"), f.col("gtinName"),
                           f.col("diets.*"))
# Concatenate all diet metadata(if any) with the ' diet,' after each diet type and apply lowercase so we can do a
# compare later to remove duplicates.  Keep UPC column as a key.  When statement to leave diet column empty when empty
# so as not to add ' diet' to the end.
pim_diets = pim_diet.withColumn('diet', concat_ws(' diet,', f.col('AYERVEDIC.name'), f.col('LOW_BACTERIA.name'),
     f.col('COELIAC.name'), f.col('DIABETIC.name'), f.col('FREE_FROM_GLUTEN.name'), f.col('GLYCEMIC.name'),
     f.col('GRAIN_FREE.name'), f.col('HALAL.name'), f.col('HGC.name'), f.col('HIGH_PROTEIN.name'),
     f.col('KEHILLA.name'), f.col('KETOGENIC.name'), f.col('KOSHER.name'), f.col('LACTOSE_FREE.name'),
     f.col('LOW_CALORIE.name'), f.col('LOW_PROTEIN.name'), f.col('LOW_SALT.name'), f.col('MACROBIOTIC.name'),
     f.col('METABOLIC.name'), f.col('NON_VEG.name'), f.col('PALEO.name'),
     f.col('PECETARIAN.name'), f.col('PLANT_BASED.name'), f.col('RAW_FOOD.name'), f.col('VEGAN.name'),
     f.col('VEGETARIAN.name'), f.col('VEG_OVO.name'), f.col('WITHOUT_BEEF.name'), f.col('WITHOUT_PORK.name')))\
     .withColumn('pim_sentence', when(f.col('diet') == '', f.col('diet')).otherwise(f.lower(concat(f.col('diet'),
                                                                                                   f.lit(' diet')))))\
     .select('upc_key', 'krogerOwnedEcommerceDescription', 'gtinName', 'pim_sentence')

# COMMAND ----------

# Break out standard upcs along with diet related metadata.  Trim UPC to standard 13 character length and rename to
# gtin_no so we can join with PIM later.
pinto_diets = pinto_prods.select(f.explode(f.col('upcs.standard')).alias('gtin_no'), f.col('dietList'))\
    .withColumn("gtin_no", expr("substring(gtin_no, 1, length(gtin_no)-1)"))\
    .select(f.col('gtin_no'), f.col('dietList.slug').alias('diet_slug'))
# Break out standard upcs along with name for a description.  Trim UPC to standard 13 character length and rename to
# gtin_no so we can join with PIM later.
pinto_names = pinto_prods.select(f.explode(f.col('upcs.standard')).alias('gtin_no'), f.col('name'))\
    .withColumn("gtin_no", expr("substring(gtin_no, 1, length(gtin_no)-1)"))\
    .select('gtin_no', 'name').distinct()
pinto_data = pinto_names.join(pinto_diets, 'gtin_no', 'inner')

# COMMAND ----------

# Join Pinto and PIM data by UPC
pinto_pim = pinto_data.join(pim_diets, pim_diets.upc_key == pinto_data.gtin_no, 'outer')
# Copy PIM description to Pinto name when Pinto name is null.
pinto_pim = pinto_pim.withColumn("name", when(pinto_pim.name.isNull(),
                                              pinto_pim.krogerOwnedEcommerceDescription).otherwise(pinto_pim.name))
# Copy Pinto name to PIM description when PIM description is null.
pinto_pim = pinto_pim.withColumn("krogerOwnedEcommerceDescription",
                                 when(pinto_pim.krogerOwnedEcommerceDescription.isNull(),
                                      pinto_pim.name).otherwise(pinto_pim.krogerOwnedEcommerceDescription))

# COMMAND ----------

# Clean data
# Concatenate Pinto diet strings with ' diet', clean up -'s and remove duplicate diet words.  Then concatenate PIM diet
# string with Pinto diet string if it exists.
sentences = pinto_pim.withColumn('diet_string', concat_ws(' diet,', f.col('diet_slug')))\
    .withColumn('clean_dashes', f.regexp_replace(f.col('diet_string'), '-', ' '))\
    .withColumn('clean_diet', f.regexp_replace(f.col('clean_dashes'), ' diet diet', ' diet'))\
    .withColumn("pimto_sentence",
                when(f.col('clean_diet') == '', f.col('pim_sentence')).otherwise(concat_ws(',', f.col('clean_diet'),
                                                                                           f.col('pim_sentence'))))\
    .select('upc_key', 'name', 'krogerOwnedEcommerceDescription', 'gtin_no', 'diet_string', 'pimto_sentence')
# Coalesce rows to get non-null values
sentences = sentences.select(f.coalesce(sentences["upc_key"],
                                        sentences["gtin_no"]).alias('gtin_no'),
                             f.col('krogerOwnedEcommerceDescription'), f.col('pimto_sentence').alias('diet_sentence'))
# Split Pinto diet data into a comma separated string
sentences = sentences.select(f.col("gtin_no"), f.col('krogerOwnedEcommerceDescription'),
                             split(f.col("diet_sentence"), ",").alias("diet_sentence_Arr"))
# Deduplicate the diet string to remove duplicate diet data.
sentences = sentences.withColumn("dedup_diet_sentence_Arr", array_distinct("diet_sentence_Arr"))\
     .withColumnRenamed('dedup_diet_sentence_Arr', 'diet_sentence')\
     .withColumn("diet_sentence", concat_ws(", ", f.col("diet_sentence")))\
     .select('gtin_no', 'krogerOwnedEcommerceDescription', 'diet_sentence')

# COMMAND ----------

# Create a "text" field or sentence that concatenates everything you want to encode(a string that has the sentence)
# Concatenate the krogerOwnedEcommerceDescription and the sentence_string together adding additional context in between
vector = sentences.select('diet_sentence', 'gtin_no', 'krogerOwnedEcommerceDescription')\
    .withColumn('sentence_string', concat_ws(',', f.col('diet_sentence')))\
    .withColumn('full_string',
                when(f.col('sentence_string') == '',
                     concat(f.col('krogerOwnedEcommerceDescription'),
                            f.lit('.'))).otherwise(concat(f.lit('The '),
                                                          f.col('krogerOwnedEcommerceDescription'),
                                                          f.lit(' is a product classified for a '),
                                                          f.col('sentence_string'), f.lit('.'))))\
    .withColumn('diet_string', f.regexp_replace(f.col('full_string'), ',,', ','))\
    .select('gtin_no', 'diet_string')
vector = vector.join(pyear_purchased_upcs, pyear_purchased_upcs.upc == vector.gtin_no)
vector = vector.select('gtin_no', 'diet_string')

# COMMAND ----------

# Loading the transformer model
model = SentenceTransformer(config.model_dir)

# COMMAND ----------

# Extract sentence field into a list
sentence = vector.rdd.map(lambda column: column.diet_string).collect()

# COMMAND ----------

# Encode vectors from sentences
# You may need a GPU cluster to do this efficiently
vectors = model.encode(sentence, normalize_embeddings=True).tolist()

# COMMAND ----------

# Join together into output dataframe
output_df = spark.createDataFrame(pd.DataFrame({"gtin_no": vector.rdd.map(lambda column: column.gtin_no).collect(),
                                                "vector": vectors}))
output_df.write.mode("overwrite").format("delta").save(config.embedded_dimensions_dir + config.product_vectors_dir +
                                                       dates.today)
