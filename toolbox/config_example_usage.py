# Databricks notebook source
"""
Demonstrates how to use the 'segmentations' and
'segmentation' class. 'segmentations' is a class
that contains all segmentations live in production. They
are grouped by ranking method and also grouped altogether for
easy access. 'segmentation' is a class that contains metadata
on the given segmentation. Such metadata includes:name, frontend name,
segment type, type, propensities, directory, and files.
"""

# COMMAND ----------

#Config where segmentations/segmentation class is stored
import toolbox.config as con

# COMMAND ----------

#Segmentations class contains segmetnations by ranking type
con.segmentations.funlo_segmentations

# COMMAND ----------

#Easy way to get all segmentations live
con.segmentations.all_segmentations

# COMMAND ----------

#Segmentation class has metadata on each segmentaion
#data includes: name, frontend name, segment type, type, propensities,
#directory, and files
segment = con.segmentation("vegetarian")
segment.name

# COMMAND ----------

#Name that appears on Prism UI
segment.frontend_name

# COMMAND ----------

#Group name that appears on Prism UI
segment.segment_type

# COMMAND ----------

#Ranking method used
segment.type

# COMMAND ----------

#Propensities used in live production
segment.propensities

# COMMAND ----------

#Directory that contains household files
segment.directory

# COMMAND ----------

#Household files present for given segmentation
segment.files

# COMMAND ----------

segment.upc_type

# COMMAND ----------

segment.upc_directory

# COMMAND ----------

segment.upc_files
