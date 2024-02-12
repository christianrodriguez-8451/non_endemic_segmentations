# Databricks notebook source
import toolbox.config as con

# COMMAND ----------

con.segmentations.fuel_segmentations

# COMMAND ----------

con.segmentations.geospatial_segmentations

# COMMAND ----------

con.segmentations.funlo_segmentations

# COMMAND ----------

con.segmentations.all_segmentations

# COMMAND ----------

segment = con.segmentation("gasoline")
segment.name

# COMMAND ----------

segment.directory

# COMMAND ----------

segment.files

# COMMAND ----------

segment.type

# COMMAND ----------

segment.propensities
