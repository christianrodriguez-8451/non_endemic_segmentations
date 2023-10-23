# Databricks notebook source
# Import service principal definitions functions, date calculation and hard coded configs from config
import resources.config as config
import src.utils as utils
import products_embedding.src.date_calcs as dates


# COMMAND ----------

# Look at acds transactions for all households the past year
acds_previous_year_transactions = \
  utils.acds.get_transactions(dates.today_year_ago,
                              dates.today, apply_golden_rules=utils.golden_rules(['customer_exclusions',
                                                                                  "fuel_exclusions"]))
acds_previous_year_transactions = acds_previous_year_transactions.where(acds_previous_year_transactions.scn_unt_qy > 0)
pyear_purchased_upcs = acds_previous_year_transactions.select(acds_previous_year_transactions.gtin_no).distinct()
pyear_purchased_upcs = pyear_purchased_upcs.select(pyear_purchased_upcs.gtin_no.alias('upc'))

pinto_path = \
  config.get_latest_modified_directory(config.azure_pinto_path)
pinto_prods = config.spark.read.parquet(pinto_path)
pim_path = \
  config.get_latest_modified_directory(config.azure_pim_core_by_cycle)
pim_core = config.spark.read.parquet(pim_path)

# COMMAND ----------

# Build a diet sentence with description and diet related meta data from PIM data source
pim_diet = pim_core.select(utils.f.col("upc_key"), utils.f.col("krogerOwnedEcommerceDescription"),
                           utils.f.col("gtinName"), utils.f.col("diets.*"))
# Concatenate all diet metadata(if any) with the ' diet,' after each diet type and apply lowercase so we can do a
# compare later to remove duplicates.  Keep UPC column as a key.  When statement to leave diet column empty when empty
# so as not to add ' diet' to the end.
pim_diets = pim_diet.withColumn('diet',
                                utils.concat_ws(' diet,',
                                                utils.f.col('AYERVEDIC.name'), utils.f.col('LOW_BACTERIA.name'),
                                                utils.f.col('COELIAC.name'), utils.f.col('DIABETIC.name'),
                                                utils.f.col('FREE_FROM_GLUTEN.name'), utils.f.col('GLYCEMIC.name'),
                                                utils.f.col('GRAIN_FREE.name'), utils.f.col('HALAL.name'),
                                                utils.f.col('HGC.name'), utils.f.col('HIGH_PROTEIN.name'),
                                                utils.f.col('KEHILLA.name'), utils.f.col('KETOGENIC.name'),
                                                utils.f.col('KOSHER.name'), utils.f.col('LACTOSE_FREE.name'),
                                                utils.f.col('LOW_CALORIE.name'), utils.f.col('LOW_PROTEIN.name'),
                                                utils.f.col('LOW_SALT.name'), utils.f.col('MACROBIOTIC.name'),
                                                utils.f.col('METABOLIC.name'), utils.f.col('NON_VEG.name'),
                                                utils.f.col('PALEO.name'), utils.f.col('PECETARIAN.name'),
                                                utils.f.col('PLANT_BASED.name'), utils.f.col('RAW_FOOD.name'),
                                                utils.f.col('VEGAN.name'), utils.f.col('VEGETARIAN.name'),
                                                utils.f.col('VEG_OVO.name'), utils.f.col('WITHOUT_BEEF.name'),
                                                utils.f.col('WITHOUT_PORK.name')))\
    .withColumn('pim_sentence',
                utils.when(utils.f.col('diet') == '',
                           utils.f.col('diet'))
                .otherwise(utils.f.lower(utils.concat(utils.f.col('diet'),
                                                      utils.f.lit(' diet'))))).select('upc_key',
                                                                                      'krogerOwnedEcommerceDescription',
                                                                                      'gtinName', 'pim_sentence')

# COMMAND ----------

# Break out standard upcs along with diet related metadata.  Trim UPC to standard 13 character length and rename to
# gtin_no so we can join with PIM later.
pinto_diets = pinto_prods.select(utils.f.explode(utils.f.col('upcs.standard')).alias('gtin_no'),
                                 utils.f.col('dietList'))\
    .withColumn("gtin_no", utils.expr("substring(gtin_no, 1, length(gtin_no)-1)"))\
    .select(utils.f.col('gtin_no'), utils.f.col('dietList.slug').alias('diet_slug'))
# Break out standard upcs along with name for a description.  Trim UPC to standard 13 character length and rename to
# gtin_no so we can join with PIM later.
pinto_names = pinto_prods.select(utils.f.explode(utils.f.col('upcs.standard')).alias('gtin_no'), utils.f.col('name'))\
    .withColumn("gtin_no", utils.expr("substring(gtin_no, 1, length(gtin_no)-1)"))\
    .select('gtin_no', 'name').distinct()
pinto_data = pinto_names.join(pinto_diets, 'gtin_no', 'inner')

# COMMAND ----------

# Join Pinto and PIM data by UPC
pinto_pim = pinto_data.join(pim_diets, pim_diets.upc_key == pinto_data.gtin_no, 'outer')
# Copy PIM description to Pinto name when Pinto name is null.
pinto_pim = pinto_pim.withColumn("name", utils.when(pinto_pim.name.isNull(),
                                              pinto_pim.krogerOwnedEcommerceDescription).otherwise(pinto_pim.name))
# Copy Pinto name to PIM description when PIM description is null.
pinto_pim = pinto_pim.withColumn("krogerOwnedEcommerceDescription",
                                 utils.when(pinto_pim.krogerOwnedEcommerceDescription.isNull(),
                                      pinto_pim.name).otherwise(pinto_pim.krogerOwnedEcommerceDescription))

# COMMAND ----------

# Clean data
# Concatenate Pinto diet strings with ' diet', clean up -'s and remove duplicate diet words.  Then concatenate PIM diet
# string with Pinto diet string if it exists.
sentences = pinto_pim.withColumn('diet_string', utils.concat_ws(' diet,', utils.f.col('diet_slug')))\
    .withColumn('clean_dashes', utils.f.regexp_replace(utils.f.col('diet_string'), '-', ' '))\
    .withColumn('clean_diet', utils.f.regexp_replace(utils.f.col('clean_dashes'), ' diet diet', ' diet'))\
    .withColumn("pimto_sentence",
                utils.when(utils.f.col('clean_diet') == '',
                           utils.f.col('pim_sentence')).otherwise(utils.concat_ws(',',
                                                                                  utils.f.col('clean_diet'),
                                                                                  utils.f.col('pim_sentence'))))\
    .select('upc_key', 'name', 'krogerOwnedEcommerceDescription', 'gtin_no', 'diet_string', 'pimto_sentence')
# Coalesce rows to get non-null values
sentences = sentences.select(utils.f.coalesce(sentences["upc_key"],
                                        sentences["gtin_no"]).alias('gtin_no'),
                             utils.f.col('krogerOwnedEcommerceDescription'),
                             utils.f.col('pimto_sentence').alias('diet_sentence'))
# Split Pinto diet data into a comma separated string
sentences = sentences.select(utils.f.col("gtin_no"), utils.f.col('krogerOwnedEcommerceDescription'),
                             utils.split(utils.f.col("diet_sentence"), ",").alias("diet_sentence_Arr"))
# Deduplicate the diet string to remove duplicate diet data.
sentences = sentences.withColumn("dedup_diet_sentence_Arr", utils.array_distinct("diet_sentence_Arr"))\
     .withColumnRenamed('dedup_diet_sentence_Arr', 'diet_sentence')\
     .withColumn("diet_sentence", utils.concat_ws(", ", utils.f.col("diet_sentence")))\
     .select('gtin_no', 'krogerOwnedEcommerceDescription', 'diet_sentence')

# COMMAND ----------

# Create a "text" field or sentence that concatenates everything you want to encode(a string that has the sentence)
# Concatenate the krogerOwnedEcommerceDescription and the sentence_string together adding additional context in between
vector = sentences.select('diet_sentence', 'gtin_no', 'krogerOwnedEcommerceDescription')\
    .withColumn('sentence_string', utils.concat_ws(',', utils.f.col('diet_sentence')))\
    .withColumn('full_string',
                utils.when(utils.f.col('sentence_string') == '',
                     utils.concat(utils.f.col('krogerOwnedEcommerceDescription'),
                            utils.f.lit('.'))).otherwise(utils.concat(utils.f.lit('The '),
                                                          utils.f.col('krogerOwnedEcommerceDescription'),
                                                          utils.f.lit(' is a product classified for a '),
                                                          utils.f.col('sentence_string'), utils.f.lit('.'))))\
    .withColumn('diet_string', utils.f.regexp_replace(utils.f.col('full_string'), ',,', ','))\
    .select('gtin_no', 'diet_string')
vector = vector.join(pyear_purchased_upcs, pyear_purchased_upcs.upc == vector.gtin_no)
vector = vector.select('gtin_no', 'diet_string')

# COMMAND ----------

# Extract sentence field into a list
sentence = vector.rdd.map(lambda column: column.diet_string).collect()

# COMMAND ----------

# Encode vectors from sentences
# You may need a GPU cluster to do this efficiently
vectors = utils.model.encode(sentence, normalize_embeddings=True).tolist()

# COMMAND ----------

# Join together into output dataframe
output_df = \
    config.spark.createDataFrame(utils.pd.DataFrame({"gtin_no":
                                                         vector.rdd.map(lambda column:
                                                                        column.gtin_no).collect(), "vector": vectors}))
output_df.write.mode("overwrite").format("delta").save(config.embedded_dimensions_dir + config.product_vectors_dir +
                                                       dates.today)
