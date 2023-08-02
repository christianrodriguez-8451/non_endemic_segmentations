# Databricks notebook source
#Read in the control file that contains our segmentaions, their UI label, their UI description, and which column has ehhn ID's.
control_fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/commodity_segments/handoff_control.csv"
control_df = spark.read.csv(control_fp, sep=",", header=True)
control_df = control_df.toPandas()
control_df

# COMMAND ----------

#Function that creates necessary formate for Steve Ruwe
def hh_writeout(segment_ui_label, segment_name, segment_ui_description, segment_filepath, ehhn_column_name="ehhn"):
  """
  """
  segment_dict = {
    "id": "",
    "groupName": "Digital Segmentations",
    "label": segment_ui_label,
    "name": segment_name,
    "description": segment_ui_description,
    "ehhnColumnName": ehhn_column_name,
    "columnName": "high_seg_code",
    "fileName": segment_filepath,
    "fileType": "PARQUET",
    "fileMask": "fiscal_week_",
    "fileDate": "LATEST",
    "filterType": "IN_LIST",
    "availableValues": [
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
        "value": "N"
      },
      {
        "label": "Lapsed",
        "value": "LP"
      }
    ],
    "mutuallyExclusive": False,
  }

  return(segment_dict)

# COMMAND ----------

#Process the control file dataframe to iterate throught the for loop easily
listw = control_df["segment_name"]
listx = control_df["segment_ui_label"]
listy = control_df["segment_ui_description"]
listz = control_df["ehhn_column_name"]
control = [[w, x, y, z] for w, x, y, z in zip(listw, listx, listy, listz)]

#Print each individual dictionary
for segment in control:
  segment_name = segment[0]
  segment_ui_label = segment[1]
  segment_ui_description = segment[2]
  ehhn_column_name = segment[3]

  segment_filepath = "/data/mart/comms/prd/krogerSegments/{}/{}_YYYYMMDD/".format(segment_name, segment_name)

  output = hh_writeout(segment_ui_label, segment_name, segment_ui_description, segment_filepath, ehhn_column_name)
  print("{}\n\n".format(output))

# COMMAND ----------


