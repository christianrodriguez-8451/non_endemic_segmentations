# Databricks notebook source
import seg

# COMMAND ----------

aiq = seg.get_seg_for_date('aiq', '20240206')
aiq.limit(20).display()

# COMMAND ----------

aiq.select('ehhn', *[col for col in aiq.columns if 'home' in col]).limit(50).display()

# COMMAND ----------

[col for col in aiq.columns if 'home' in col]
