from commodity_segmentations.config import percentile_segmentations
from pyspark.dbutils import DBUtils

#Class that holds segmentations live in production or are planned to be in production
#As new segmentations productionize, make sure to add it in the appropiate segmentation list.
class segmentations:
  funlo_segmentations = [
    "free_from_gluten", "grain_free", "healthy_eating",
    "ketogenic", "kidney-friendly", "lactose_free", "low_bacteria", "paleo",
    "vegan", "vegetarian", "beveragist", "breakfast_buyers", "hispanic_cuisine",
    "low_fodmap", "mediterranean_diet", "organic", "salty_snackers",
    "non_veg", "low_salt", "low_protein", "heart_friendly", "macrobiotic",
    "high_protein",
  ]
  percentile_segmentations = percentile_segmentations
  fuel_segmentations = ["gasoline", "gasoline_premium_unleaded", "gasoline_unleaded_plus", "gasoline_reg_unleaded"]
  geospatial_segmentations = ["roadies", "travelers"]
  all_segmentations = funlo_segmentations + percentile_segmentations + fuel_segmentations + geospatial_segmentations

#TODO: Make a class that groups the segmentations by how their UPC lists
# are generated. This will make it easy to track their UPC lists too.  

def get_type(segmentation_name):
  """
  """
  if segmentation_name in segmentations.funlo_segmentations:
    segmentation_type = "funlo"
    
  elif segmentation_name in segmentations.percentile_segmentations:
    segmentation_type = "percentile"

  elif segmentation_name in segmentations.fuel_segmentations:
    segmentation_type = "fuel"

  elif segmentation_name in segmentations.geospatial_segmentations:
    segmentation_type = "geospatial"

  else:
    message = (
      "ERROR!!!"
    )
    raise ValueError(message)

  return(segmentation_type)

def get_directory(segmentation_name):
  """
  """
  segmentation_type = get_type(segmentation_name)

  if segmentation_type == "funlo":
    segmentation_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/embedded_dimensions/customer_data_assets/segment_behavior/segmentation/modality={}/".format(segmentation_name)

  elif segmentation_type == "percentile":
    segmentation_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/percentile_segmentations/{}/".format(segmentation_name)

  elif segmentation_type == "fuel":
    segmentation_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/fuel_segmentations/{}/".format(segmentation_name)

  elif segmentation_type == "geospatial":
    segmentation_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/geospatial/{}/".format(segmentation_name)

  return(segmentation_dir)

def get_files(segmentation_name):
  """
  """
  segmentation_type = get_type(segmentation_name)
  segmentation_dir = get_directory(segmentation_name)

  dbutils = DBUtils()
  if segmentation_type == "funlo":
    files = dbutils.fs.ls(segmentation_dir)
    files = [x[1] for x in files if "stratum_week=" in x[1]]

  elif segmentation_type in ["percentile", "fuel", "geospatial"]:
    files = dbutils.fs.ls(segmentation_dir)
    files = [x[1] for x in files if "{}_".format(segmentation_name) in x[1]]

  files.sort()
  return(files)

def get_propensity_composition(segmentation_name):
  """
  """
  propensity_dict = {
    "free_from_gluten": ["H"],
    "grain_free": ["H"],
    "healthy_eating": ["H"],
    "heart_friendly": ["H"],
    "ketogenic": ["H"],
    "kidney-friendly": ["H"],
    "lactose_free": ["H", "M", "L"],
    "low_bacteria": ["H", "M"],
    "low_protein": ["H", "M"],
    "low_salt": ["H", "M"],
    "paleo": ["H", "M"],
    "vegan": ["H"],
    "vegetarian": ["H"],
    "non_veg": ["H", "M"],
    "macrobiotic": ["H", "M"],
    "salty_snackers": ["H", "M"],
    "organic": ["H", "M"],
    "mediterranean_diet": ["H"],
    "low_fodmap": ["H"],
    "beveragist": ["H", "M"],
    "hispanic_cuisine": ["H", "M"],
    "breakfast_buyers": ["H", "M"],
    "gasoline_premium_unleaded": ["H", "M"],
    "gasoline_unleaded_plus": ["H", "M"],
    "gasoline_reg_unleaded": ["H"],
    "gasoline": ["H"],
    "beautists": ["H"],
    "beverage_enchancers": ["H"],
    "summer_bbq": ["H"],
    "convenient_breakfast": ["H"],
    "energy_beveragist": ["H"],
    "houses_with_children": ["H", "M", "L"],
    "high_protein": ["H"],
    "artsy_folk": ["H", "M", "L"],
    "asian_cuisine": ["H"],
    "camper": ["H", "M", "L"],
    "christmas": ["H", "M", "L"],
    "easter": ["H", "M", "L"],
    "halloweeners": ["H", "M", "L"],
    "roadies": ["H", "M"],
    "travelers": ["H", "M"],
    "back-to-school": ["H", "M"],
    "fitness_enthusiast": ["H"],
    'fruit_and_natural_snackers': ["H"],
    'indoor_décor_and_gardening': ["H", "M"],
    'isotonic_beveragist': ["H"],
    'meat_snackers': ["H"],
    'natural_beveragist': ["H", "M"],
    'outdoor_décor_and_gardening': ["H", "M"],
    'pizza_meal_households': ["H"],
    'reader': ["H", "M"],
    'casual_auto_fixers': ["H", "M", "L"],
  }
  return(propensity_dict[segmentation_name])

#Class for each segmentation that contains the following 
#attributes: name, type, propensities, directory, files
#Example usage: segmentation("free_from_gluten")
class segmentation:
  def __init__(self, segmentation_name):    
    self.name = segmentation_name
    self.type = get_type(segmentation_name)
    self.propensities = get_propensity_composition(segmentation_name)
    self.directory = get_directory(segmentation_name)
    self.files = get_files(segmentation_name)
    #self.upc_directory = get_upc_directory(segmentation_name)
    #self.upc_files = get_upc_files(segmentation_name)

#TODO: create read-in functions for household and UPC files?
def read_in(segmentation_name, cycle=None):
  """
  """
  return(None)

#TODO: Create below functions for UPCs
def get_upc_directory(segmentation_name):
  """
  """
  #Need upc_segmentations class
  return(None)

def get_upc_files(segmentation_name):
  """
  """
  #Need upc_segmentations class
  return(None)

