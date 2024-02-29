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
    "high_protein", "juicing_beveragust", "without_pork", "pescetarian",
    "raw_food",
  ]
  percentile_segmentations = percentile_segmentations
  fuel_segmentations = ["gasoline", "gasoline_premium_unleaded", "gasoline_unleaded_plus", "gasoline_reg_unleaded"]
  geospatial_segmentations = ["roadies", "travelers", "metropolitan", "micropolitan"]
  all_segmentations = funlo_segmentations + percentile_segmentations + fuel_segmentations + geospatial_segmentations
  all_segmentations.sort()

#Dictionary that contains name on Prism UI, group name on Prism UI,
#and propensities used in the json templates.
audience_dict = {
  "free_from_gluten": {
    "frontend_name": "Gluten-Free Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "grain_free": {
    "frontend_name": "Grain-Free Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "healthy_eating": {
    "frontend_name": "Nutrient Dense Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "heart_friendly": {
    "frontend_name": "Wholesome Food Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "ketogenic": {
    "frontend_name": "Keto Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "kidney-friendly": {
    "frontend_name": "Lean Meat Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "lactose_free": {
    "frontend_name": "Lactose-Free Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M", "L"],
  },
  "low_bacteria": {
    "frontend_name": "Low Bacteria Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "low_protein": {
    "frontend_name": "Low Protein Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "low_salt": {
    "frontend_name": "Low Salt Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "macrobiotic": {
    "frontend_name": "Macro Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "non_veg": {
    "frontend_name": "Non-Vegetarian Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "paleo": {
    "frontend_name": "Paleo Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "vegan": {
    "frontend_name": "Vegan Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "vegetarian": {
    "frontend_name": "Vegetarian Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "beveragist": {
    "frontend_name": "Beverage Product Buyers",
    "segment_type": "Food & Beverage",
    "ranking_method": "funlo",
    "propensity_compisition": ["H", "M"],
  },
  "breakfast_buyers": {
    "frontend_name": "Breakfast Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "hispanic_cuisine": {
    "frontend_name": "Hispanic Cuisine Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "juicing_beveragust": {
    "frontend_name": "Juicing Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "organic": {
    "frontend_name": "Organic Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "salty_snackers": {
    "frontend_name": "Snack Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "high_protein": {
    "frontend_name": "High Protein Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "raw_food": {
    "frontend_name": "Raw Food Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "pescetarian": {
    "frontend_name": "Fish Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "without_pork": {
    "frontend_name": "Pork-Free Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "mediterranean_diet": {
    "frontend_name": "Mediterranean Cuisine Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "low_fodmap": {
    "frontend_name": "Fermentable-Free FODMAP Friendly Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "beverage_enchancers": {
    "frontend_name": "Beverage Enhancer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "convenient_breakfast": {
    "frontend_name": "Convenient Breakfast Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "energy_beveragist": {
    "frontend_name": "Energy Beverage Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "asian_cuisine": {
    "frontend_name": "Asian Cuisine Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "pizza_meal_households": {
    "frontend_name": "Pizza Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "natural_beveragist": {
    "frontend_name": "Natural Beverage Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "fruit_and_natural_snackers": {
    "frontend_name": "Fruit Snack Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "isotonic_beveragist": {
    "frontend_name": "Isotonic Beverage Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "meat_snackers": {
    "frontend_name": "Protein Snack Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "fitness_enthusiast": {
    "frontend_name": "",
    "segment_type": "Household",
    "propensity_compisition": ["H"],
  },
  "casual_auto_fixers": {
    "frontend_name": "Casual Automobile Fixers",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M"],
  },
  "summer_bbq": {
    "frontend_name": "Grilling Enthusiasts",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M"],
  },
  "artsy_folk": {
    "frontend_name": "Arts & Crafts Enthusiasts",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M", "L"],
  },
  "camper": {
    "frontend_name": "Camping Enthusiasts",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M", "L"],
  },
  "indoor_decor_and_gardening": {
    "frontend_name": "Indoor Gardening Buyers",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M"],
  },
  "outdoor_decor_and_gardening": {
    "frontend_name": "Outdoor Gardening Buyers",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M"],
  },
  "reader": {
    "frontend_name": "Reading Enthusiasts",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M"],
  },
  "gasoline": {
    "frontend_name": "High Volume Fuel Fillers",
    "segment_type": "Household",
    "propensity_compisition": ["H"],
  },
  "gasoline_premium_unleaded": {
    "frontend_name": "Premium Unleaded Fuel Buyers",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M"],
  },
  "gasoline_reg_unleaded": {
    "frontend_name": "Regular Unleaded Fuel Buyers",
    "segment_type": "Household",
    "propensity_compisition": ["H"],
  },
  "gasoline_unleaded_plus": {
    "frontend_name": "Unleaded Plus Fuel Buyers",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M"],
  },
  "christmas": {
    "frontend_name": "Winter Holiday Enthusiasts",
    "segment_type": "Seasonal",
    "propensity_compisition": ["H", "M", "L"],
  },
  "easter": {
    "frontend_name": "Spring Holiday Enthusiasts",
    "segment_type": "Seasonal",
    "propensity_compisition": ["H", "M", "L"],
  },
  "halloweeners": {
    "frontend_name": "Halloween Enthusiasts",
    "segment_type": "Seasonal",
    "propensity_compisition": ["H", "M", "L"],
  },
  "back-to-school": {
    "frontend_name": "Back-to-School Product Shoppers",
    "segment_type": "Event",
    "propensity_compisition": ["H", "M"],
  },
  "roadies": {
    "frontend_name": "",
    "segment_type": "Event",
    "propensity_compisition": ["H", "M"],
  },
  "travelers": {
    "frontend_name": "",
    "segment_type": "Event",
    "propensity_compisition": ["H", "M"],
  },
  "metropolitan": {
    "frontend_name": "Metropolitans",
    "segment_type": "Geo-Specific",
    "propensity_compisition": ['H'],
  },
  "micropolitan": {
    "frontend_name": "Micropolitans",
    "segment_type": "Geo-Specific",
    "propensity_compisition": ['H'],
  },
  "beautists": {
    "frontend_name": "Beauty Product Buyers",
    "segment_type": "Beauty/Personal Care",
    "propensity_compisition": ['H'],
  },
  "houses_with_children": {
    "frontend_name": "Household With Children",
    "segment_type": "Life Stage",
    "propensity_compisition": ["H", "M", "L"],
  },
}

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
      "{} is not present in any of the lists contained withing the 'segmentations' class." +
      "Make sure to update the 'segmentations' class or config files as appropiate."
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

#Class for each segmentation that contains the following 
#attributes: name, frontend name, segment type, type, propensities,
#directory, files.
#Example usage: segmentation("free_from_gluten")
class segmentation:
  def __init__(self, segmentation_name):    
    self.name = segmentation_name
    self.frontend_name = audience_dict[segmentation_name]["frontend_name"]
    self.segment_type = audience_dict[segmentation_name]["segment_type"]
    self.type = get_type(segmentation_name)
    self.propensities = audience_dict[segmentation_name]["propensity_compisition"]
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

