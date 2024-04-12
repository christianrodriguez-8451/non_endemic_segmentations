from products_embedding.config import sentences_dict as em_dict, funlo_segmentations
from commodity_segmentations.config import percentile_segmentations, commodity_segmentations as cs_dict, sensitive_segmentations as ss_dict
from pyspark.dbutils import DBUtils

#Class that holds segmentations live in production or are planned to be in production
#As new segmentations productionize, make sure to add it in the appropiate segmentation list.
class segmentations:
  embedding_segmentations = list(em_dict.keys())
  commodity_segmentations = list(cs_dict.keys())
  funlo_segmentations = funlo_segmentations
  percentile_segmentations = percentile_segmentations
  fuel_segmentations = ["gasoline", "gasoline_premium_unleaded", "gasoline_unleaded_plus", "gasoline_reg_unleaded"]
  geospatial_segmentations = ["roadies", "travelers", "metropolitan", "micropolitan"]
  sensitive_segmentations = list(ss_dict.keys())
  all_segmentations = (
    funlo_segmentations + percentile_segmentations + fuel_segmentations + geospatial_segmentations + embedding_segmentations
    + commodity_segmentations + sensitive_segmentations
  )
  all_segmentations = list(set(all_segmentations))
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
    "frontend_name": "Fitness Enthusiasts",
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
    "frontend_name": "Road Trippers",
    "segment_type": "Event",
    "propensity_compisition": ["H", "M"],
  },
  "travelers": {
    "frontend_name": "Travel Enthusiasts",
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
    "frontend_name": "Children Product Buyers",
    "segment_type": "Life Stage",
    "propensity_compisition": ["H", "M"],
  },
  "metabolic": {
    "frontend_name": "Metabolic Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "scotch": {
    "frontend_name": "Scotch Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "vodka": {
    "frontend_name": "Vodka Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "rum": {
    "frontend_name": "Rum Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "tequila": {
    "frontend_name": "Tequila Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "gin": {
    "frontend_name": "Gin Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "cognac": {
    "frontend_name": "Cognac Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "brandy": {
    "frontend_name": "Brandy Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "bourbon": {
    "frontend_name": "Bourbon Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "whiskey": {
    "frontend_name": "Whiskey Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "liquor": {
    "frontend_name": "Liquor Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "ayervedic": {
    "frontend_name": "Ayurvedic Cuisine Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "low_calorie": {
    "frontend_name": "Low-Calorie Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "engine_2": {
    "frontend_name": "Nutrient-Rich Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "glycemic": {
    "frontend_name": "Low Glycemic Lifestyle Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "plant_based_whole_foods": {
    "frontend_name": "Plant-Powered Whole Foods Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "ovo-vegetarians": {
    "frontend_name": "Ovo-Vegetarian Pantry Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "without_beef": {
    "frontend_name": "Meat-Free Grocery Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "plant_based": {
    "frontend_name": "Veggie-Focused Food Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
  },
  "domestic_beer": {
    "frontend_name": "Domestic Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "domestic_below_premium_beer": {
    "frontend_name": "Domestic Sub Premium Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "domestic_premium_beer": {
    "frontend_name": "Domestic Premium Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "domestic_above_premium_beer": {
    "frontend_name": "Domestic Premium Plus Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "imported_beer": {
    "frontend_name": "Imported Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "imported_asian_beer": {
    "frontend_name": "Imported Asian Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "imported_canadian_beer": {
    "frontend_name": "Imported Canadian Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "imported_european_beer": {
    "frontend_name": "Imported European Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "imported_hispanic_beer": {
    "frontend_name": "Imported Hispanic Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "craft_beer": {
    "frontend_name": "Craft Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "craft_national_beer": {
    "frontend_name": "National Craft Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "craft_local_beer": {
    "frontend_name": "Local Craft Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "beer": {
    "frontend_name": "Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
  },
  "fmb_and_hard_seltzers": {
    "frontend_name": "Hard Seltzer & Malt Beverage Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
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

  elif segmentation_name in segmentations.sensitive_segmentations:
    segmentation_type = "sensitive"

  else:
    message = (
      "{} is not present in any of the lists contained within the 'segmentations' class.".format(segmentation_name) +
      " Make sure to update the 'segmentations' class or config files as appropiate."
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

  elif segmentation_type == "sensitive":
    segmentation_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/sensitive_segmentations/{}/".format(segmentation_name)

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

  elif segmentation_type in ["percentile", "fuel", "geospatial", "sensitive"]:
    files = dbutils.fs.ls(segmentation_dir)
    files = [x[1] for x in files if "{}_".format(segmentation_name) in x[1]]

  files.sort()
  return(files)

def get_upc_type(segmentation_name):
  """
  """
  search_keys = list(em_dict.keys())
  comm_keys = list(cs_dict.keys())

  if (segmentation_name in search_keys) and (segmentation_name in comm_keys):
    message = (
      "{} is in both of the control dictionaries for ...."
    )
    raise ValueError(message)
  elif segmentation_name in search_keys:
    upc_type = "search_embedding"
  elif segmentation_name in comm_keys:
    upc_type = "commodities_subcommodities"
  else:
    upc_type = None

  return(upc_type)


def get_upc_directory(segmentation_name):
  """
  """
  upc_type = get_upc_type(segmentation_name)

  if upc_type == "search_embedding":
    upc_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/embedded_dimensions/diet_query_embeddings/"
  elif upc_type == "commodities_subcommodities":
    upc_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/upc_lists/{}/".format(segmentation_name)
  else:
    upc_dir = None

  return(upc_dir)

def get_upc_files(segmentation_name):
  """
  """
  upc_type = get_upc_type(segmentation_name)
  upc_dir = get_upc_directory(segmentation_name)

  dbutils = DBUtils()
  if upc_type == "search_embedding":
    dirs = dbutils.fs.ls(upc_dir)
    dirs = [x[1] for x in dirs if "cycle_date=" in x[1]]

    #The upc files for search embeddings are separated across cycles
    files = []
    for d in dirs:
      d_files = dbutils.fs.ls(upc_dir + d)
      d_files = [x[1] for x in d_files]
      d_files = [str(x).strip("/") for x in d_files]

      if segmentation_name in d_files:
        files += [d + segmentation_name + "/"]

    files.sort()
  elif upc_type == "commodities_subcommodities":
    files = dbutils.fs.ls(upc_dir)
    files = [x[1] for x in files if "{}_".format(segmentation_name) in x[1]]
    files.sort()
  else:
    files = None

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
    self.upc_type = get_upc_type(segmentation_name)
    self.upc_directory = get_upc_directory(segmentation_name)
    self.upc_files = get_upc_files(segmentation_name)

#TODO: create read-in functions for household and UPC files?
def read_in(segmentation_name, cycle=None):
  """
  """
  return(None)

