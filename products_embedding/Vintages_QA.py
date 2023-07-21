# Databricks notebook source
keto_hh_vintage_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/vintages/hh_ketogenic/vintage_week=20230503" 
keto_hh_vintage = spark.read.parquet(keto_hh_vintage_path)
keto_sales_vintage_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/vintages/sales_ketogenic/fiscal_week=20230503" 
keto_sales_vintage = spark.read.parquet(keto_sales_vintage_path)

keto_segment_weight_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/segment_behavior/weights/modality=ketogenic/stratum_week=20230504" 
keto_segment_weight = spark.read.format("delta").load(keto_segment_weight_path)

keto_segment_segmentation_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/segment_behavior/segmentation/modality=ketogenic/stratum_week=20230504" 
keto_segment_segmentation = spark.read.format("delta").load(keto_segment_segmentation_path)
keto_query_embedding_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/diet_query_embeddings/cycle_date=2023-06-04/ketogenic" 
keto_query_embedding = spark.read.format("delta").load(keto_query_embedding_path)


vegetarian_hh_vintage_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/vintages/hh_vegetarian/vintage_week=20230503" 
vegetarian_hh_vintage = spark.read.parquet(vegetarian_hh_vintage_path)
vegetarian_sales_vintage_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/vintages/sales_vegetarian/fiscal_week=20230503" 
vegetarian_sales_vintage = spark.read.parquet(vegetarian_sales_vintage_path)
vegetarian_segment_weight_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/segment_behavior/weights/modality=vegetarian/stratum_week=20230504" 
vegetarian_segment_weight = spark.read.format("delta").load(vegetarian_segment_weight_path)
vegetarian_segment_segmentation_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/segment_behavior/segmentation/modality=vegetarian/stratum_week=20230504" 
vegetarian_segment_segmentation = spark.read.format("delta").load(vegetarian_segment_segmentation_path)
vegetarian_query_embedding_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/diet_query_embeddings/cycle_date=2023-06-11/vegetarian" 
vegetarian_query_embedding = spark.read.format("delta").load(vegetarian_query_embedding_path)

vegan_hh_vintage_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/vintages/hh_vegan/vintage_week=20230503" 
vegan_hh_vintage = spark.read.parquet(vegan_hh_vintage_path)
vegan_sales_vintage_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/vintages/sales_vegan/fiscal_week=20230503" 
vegan_sales_vintage = spark.read.parquet(vegan_sales_vintage_path)
vegan_segment_weight_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/segment_behavior/weights/modality=vegan/stratum_week=20230504" 
#vegan_segment_weight = spark.read.format("delta").load(vegan_segment_weight_path)
vegan_segment_segmentation_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/customer_data_assets/segment_behavior/segmentation/modality=vegan/stratum_week=20230504" 
vegan_segment_segmentation = spark.read.format("delta").load(vegan_segment_segmentation_path)
vegan_query_embedding_path = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/diet_query_embeddings/cycle_date=2023-06-11/vegan" 
vegan_query_embedding = spark.read.format("delta").load(vegan_query_embedding_path)


from effodata import ACDS, golden_rules
import kayday as kd

# this doesn't seem to work like get_spark_session... maybe refactor
acds = ACDS(use_sample_mart=False) #i will need to specify no sample mart

# COMMAND ----------

keto_hh_vintage.display()
keto_sales_vintage.display()
keto_segment_weight.display()
keto_segment_segmentation.where(keto_segment_segmentation.ehhn == '17015418').display()
keto_query_embedding.display()

# COMMAND ----------

keto_segment_segmentation.where(keto_segment_segmentation.segment == 'H').count()#4,382,006

# COMMAND ----------

# MAGIC %md
# MAGIC ####Samantha - persona vegetarian and pescatarian. 

# COMMAND ----------

#Samantha - persona ketogenic.  
keto_segment_segmentation.where(keto_segment_segmentation.ehhn == '17015418').display()
keto_sales_vintage.where(keto_sales_vintage.ehhn == '17015418').display()
keto_segment_weight.where(keto_segment_weight.ehhn == '17015418').display()

# COMMAND ----------

samantha_q4_trans = acds.get_transactions(start_date="20230212", end_date="20230603", query_filters=["ehhn = '17015418'"]).limit(1000)
samantha_q4_gtin = samantha_q4_trans.select(samantha_q4_trans.gtin_no).distinct()
samantha_q4_keto_gtin = samantha_q4_gtin.join(keto_query_embedding, keto_query_embedding.gtin_no == samantha_q4_gtin.gtin_no).distinct()
samantha_q4_keto_gtin.display()

# COMMAND ----------

keto_upcs = samantha_q4_keto_gtin.rdd.map(lambda column: column.gtin_no).collect()

import pandas as pd
def display_upcs(x):
  ldf=pd.DataFrame(x,columns=['UPC'])
  ldf['IMG']=ldf['UPC'].map(lambda x: f'<img src="https://www.kroger.com/product/images/medium/front/{x}">' )#if x.isnumeric() else '')
  return ldf.T.style

display_upcs(keto_upcs)

# COMMAND ----------

carrots

# COMMAND ----------



# COMMAND ----------

#Samantha - persona vegetarian and pescatarian.  
vegetarian_segment_segmentation.where(vegetarian_segment_segmentation.ehhn == '17015418').display()
vegetarian_sales_vintage.where(vegetarian_sales_vintage.ehhn == '17015418').display()
vegetarian_segment_weight.where(vegetarian_segment_weight.ehhn == '17015418').display()

# COMMAND ----------

samantha_q4_vegetarian_gtin = samantha_q4_gtin.join(vegetarian_query_embedding, vegetarian_query_embedding.gtin_no == samantha_q4_gtin.gtin_no).distinct()
samantha_q4_vegetarian_gtin.display()

# COMMAND ----------

vegetarian_upcs = samantha_q4_vegetarian_gtin.rdd.map(lambda column: column.gtin_no).collect()

import pandas as pd
def display_upcs(x):
  ldf=pd.DataFrame(x,columns=['UPC'])
  ldf['IMG']=ldf['UPC'].map(lambda x: f'<img src="https://www.kroger.com/product/images/medium/front/{x}">' )#if x.isnumeric() else '')
  return ldf.T.style

display_upcs(vegetarian_upcs)

# COMMAND ----------

vegetarian_segment_segmentation.where(vegetarian_segment_segmentation.segment == 'H').count()

# COMMAND ----------


