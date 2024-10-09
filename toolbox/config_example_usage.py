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

#Audiences created by filtering on the given level of hierarchy and deploying regex search patterns
con.segmentations.regex_segmentations

# COMMAND ----------

#Audiences created by leveraging AIQ data
con.segmentations.generation_segmentations

# COMMAND ----------

#Audiences created by simply looking at department purchases
con.segmentations.department_segmentations

# COMMAND ----------

#Audiences created by simply looking at department purchases
con.segmentations.seasonal_segmentations

# COMMAND ----------

#Easy way to get all segmentations live
con.segmentations.all_segmentations

# COMMAND ----------

#Segmentation class has metadata on each segmentaion
#data includes: name, frontend name, segment type, type, propensities,
#directory, and files
segment = con.segmentation("valentines_day")
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

#Method used to create the UPC list
segment.upc_type

# COMMAND ----------

#Directory where UPC files are stored
segment.upc_directory

# COMMAND ----------

#Filenames of available UPC files
segment.upc_files

# COMMAND ----------

#Using the class' attributes to read in the latest UPC file

for audience in con.segmentations.seasonal_segmentations:
  #Create the segment class for vegetarian
  segment = con.segmentation(audience)
  #Directory where upc lists are stored
  upc_dir = segment.upc_directory
  #Pull the filename for the latest UPC list. For audiences that are UPC based, most of their
  #UPC lists are refreshed on a weekly basis and stored away
  #upc_fn = segment.upc_files[-1]
  #Create the filepath for where the UPC list is stored
  #upc_fp = upc_dir + upc_fn
  #The UPC lists are stored as delta files
  upc_df = spark.read.format("delta").load(upc_fp)
  upc_df.show(10, truncate=False)

# COMMAND ----------

#Tags used on the Pre-Built Audience UI
segment.tags

# COMMAND ----------