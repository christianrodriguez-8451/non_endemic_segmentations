# Databricks notebook source
"""
Creates the mongDB json files given the segmentation's backend_name,
frontend_name, description, and groupName. Also creates the on-premise
json files given the segmentation's backend_name, frontend_name, description,
segmentationId, and selectedValues.

To do:
  1) Re-factor the functions so that the mongoDB and on-premise json files
  are created in a single pass. Do this by making segmentationId an argument
  for mongodb_template. (DONE)

  2) Write out the json files using PySpark (Steven Martz's code). (DONE)

  4) Write out the json files to the egress directory and confirm they are
  moved.

  5) Create an API on top of this so that you can create the json files
  by only inputting backend_name, groupName, and selectedValues. Marketing
  will be responsivle for pasting in name and description in the json files.

  6) Make API dynamic so that instead of selectedValues, it will ask for
  columnName instead. Given the columName, it goes out to read example
  input data and asks the user to explicitly define the labels for the values
  it finds in the column.

"""

# COMMAND ----------

import random
import string
import json
import uuid
import shutil

def draw_id(taken_ids = []):
  """
  Randomly draws a UUID in string format.
  """
  #To kick off the while loop
  id_str = "token"
  taken_ids += ["token"]

  #Drawing a random UUID
  #A UUID is an ID of 32 randomly drawn characters of a-f and 0-9
  #The UUID format is XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
  while id_str in taken_ids:
      values = ["a", "b", "c", "d", "e", "f"] + [str(x) for x in range(0, 9+1)]
      id_piece1 = "".join(random.sample(values, 8))
      id_piece2 = "".join(random.sample(values, 4))
      id_piece3 = "".join(random.sample(values, 4))
      id_piece4 = "".join(random.sample(values, 4))
      id_piece5 = "".join(random.sample(values, 12))
      id_str = "-".join([id_piece1, id_piece2, id_piece3, id_piece4, id_piece5])

  return(id_str)

def mongodb_template(
  backend_name,
  frontend_name,
  description,
  groupName,
  segmentationId,
  columnName,
  availableValues,
):
  """
  Creates an audience template for the mongoDB side.
  """
  backend_name = backend_name.lower().strip()

  #Template from engineering
  template = {
    "_id": {"$uuid": segmentationId.replace("-", "")},
    "groupName": groupName,
    "name": backend_name.upper().strip(),
    "label": frontend_name.strip(),
    "description": description.strip(),
    "columnName": columnName,
    "fileName": f"/data/mart/comms/prd/krogerSegments/{backend_name}/",
    "fileType": "PARQUET",
    "fileMask": f"{backend_name}_",
    "fileDate": "LATEST",
    "_class": "PredefinedSegmentation",
    "mutuallyExclusive": False,
    "availableValues": availableValues,
    "filterType": "IN_LIST",
    "isInternal": False,
    "ehhnColumnName": "ehhn"
  }
  return(template)

def onprem_template(
    backend_name,
    frontend_name,
    description,
    segmentationId,
    selectedValues,
    ):
    """
    Creates an audience template for the on-premise side.
    """
    template = {
    "name": frontend_name.strip(),
    "description": description.strip(),
    "cells": [
        {
            "type": "PRE_DEFINED_SEGMENTATION",
            "predefinedSegment": {
                "segmentationId": segmentationId,
                "selectedValues": selectedValues,
                "segmentationName": backend_name.upper().strip(),
            },
            "order": 0
        }
    ],
    "combineOperator": "AND",
    "krogerSegment": True,
    "id": draw_id(),
    }
    return(template)
  
def writeout_json(json_dict, output_dir, output_fn):
  """
  Writes out the inputted dictionary as a json file at
  the inputted output directory and output filename.
  """

  #Convert the dictionary to an RDD so that it can be written as a text file
  json_string = json.dumps(json_dict)
  json_rdd = spark.sparkContext.parallelize([json_string])
  json_rdd = json_rdd.coalesce(1)
  json_rdd.saveAsTextFile(output_dir + "_temporary")

  #Extract the consolidated partition and delete the temp folder
  target_fp = dbutils.fs.ls(output_dir + "_temporary")[1][0]
  dbutils.fs.mv(target_fp, output_dir + output_fn)
  dbutils.fs.rm(output_dir + "_temporary", True)

class DefaultParameters:
  def __init__(self):
    self.frontend_name = "MARKETING - INSERT FINAL NAME HERE"
    self.description = "MARKETING - INSERT FINAL DESCRIPTION HERE"
    self.columnName = "segment"
    self.availableValues = [
      {
        "label": "High",
        "value": "H"
      },
      {
        "label": "Medium",
        "value": "M"
      },
      {
        "label": "Low",
        "value": "L"
      },
      {
        "label": "New",
        "value": "new"
      },
      {
        "label": "Lapsed",
        "value": "lapsed"
      },
      {
        "label": "Inconsistent",
        "value": "inconsistent"
      }
    ]
    self.selectedValues = ["H"]
    self.output_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/"

def main(
  backend_name,
  groupName,
  frontend_name = DefaultParameters().frontend_name,
  description = DefaultParameters().description,
  columnName=DefaultParameters().columnName,
  availableValues=DefaultParameters().availableValues,
  selectedValues=DefaultParameters().selectedValues,
  output_dir=DefaultParameters().output_dir,
  ):
  """
  Creates and writes out the templates for MongoDB and
  On-Premise in a single pass. It is important to do this
  in a single pass because there is a segmentationID that
  links both templates.
  """

  id_str = draw_id()

  mongo_json = mongodb_template(
    backend_name=backend_name,
    groupName=groupName,
    frontend_name=frontend_name,
    description=description,
    columnName=columnName,
    availableValues=availableValues,
    segmentationId=id_str,
  )
  output_fn = "mongo_" + backend_name + ".json"
  writeout_json(json_dict=mongo_json, output_dir=output_dir, output_fn=output_fn)

  onprem_json = onprem_template(
    backend_name=backend_name,
    frontend_name=frontend_name,
    description=description,
    selectedValues=selectedValues,
    segmentationId=id_str,
  )
  output_fn = backend_name + ".json"
  writeout_json(json_dict=onprem_json, output_dir=output_dir, output_fn=output_fn)

# COMMAND ----------

#Example Usage
import toolbox.config as con
seg = "high_protein"
segment = con.segmentation(seg)

main(
  backend_name=seg,
  groupName="Food & Beverage",
  frontend_name="High Protein Product Buyers",
  description="Buyers who prefer to purchase products that contain protein branded descriptions including protein bars, protein powdered supplements, meat snacks, smoked sausages and protein shakes and high protein vegetables.",
  selectedValues=segment.propensities,
  #output_dir="abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/egress/",
)
