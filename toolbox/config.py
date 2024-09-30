from products_embedding.config import sentences_dict as em_dict, funlo_segmentations
from commodity_segmentations.config import percentile_segmentations, commodity_segmentations as cs_dict, sensitive_segmentations as ss_dict, regex_segmentations as reg_dict
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
  regex_segmentations = list(reg_dict.keys())
  generation_segmentations = ["boomers", "gen_x", "millennials", "gen_z"]
  department_segmentations = ["deli_and_bakery", "meat", "produce", "grocery", "food"]
  all_segmentations = (
    funlo_segmentations + percentile_segmentations + fuel_segmentations + geospatial_segmentations + embedding_segmentations
    + commodity_segmentations + sensitive_segmentations + regex_segmentations + generation_segmentations + department_segmentations
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
    "tags": ["Lifestyle"],
  },
  "grain_free": {
    "frontend_name": "Grain-Free Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "healthy_eating": {
    "frontend_name": "Nutrient Dense Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Food"],
  },
  "heart_friendly": {
    "frontend_name": "Wholesome Food Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Food"],
  },
  "ketogenic": {
    "frontend_name": "Keto Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "kidney-friendly": {
    "frontend_name": "Lean Meat Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Food"],
  },
  "lactose_free": {
    "frontend_name": "Lactose-Free Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M", "L"],
    "tags": ["Lifestyle"],
  },
  "low_bacteria": {
    "frontend_name": "Low Bacteria Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Food"],
  },
  "low_protein": {
    "frontend_name": "Low Protein Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Food"],
  },
  "low_salt": {
    "frontend_name": "Low Salt Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Food"],
  },
  "macrobiotic": {
    "frontend_name": "Macro Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Food"],
  },
  "non_veg": {
    "frontend_name": "Non-Vegetarian Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Food"],
  },
  "paleo": {
    "frontend_name": "Paleo Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "vegan": {
    "frontend_name": "Vegan Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "vegetarian": {
    "frontend_name": "Vegetarian Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Lifestyle"],
  },
  "beveragist": {
    "frontend_name": "Beverage Product Buyers",
    "segment_type": "Food & Beverage",
    "ranking_method": "funlo",
    "propensity_compisition": ["H", "M"],
    "tags": ["Beverage"],
  },
  "breakfast_buyers": {
    "frontend_name": "Breakfast Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Food"],
  },
  "hispanic_cuisine": {
    "frontend_name": "Hispanic Cuisine Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Cultural"],
  },
  "juicing_beveragust": {
    "frontend_name": "Juicing Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "organic": {
    "frontend_name": "Organic Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Food"],
  },
  "salty_snackers": {
    "frontend_name": "Snack Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Food"],
  },
  "high_protein": {
    "frontend_name": "High Protein Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Food"],
  },
  "raw_food": {
    "frontend_name": "Raw Food Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Food"],
  },
  "pescetarian": {
    "frontend_name": "Fish Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Food"],
  },
  "without_pork": {
    "frontend_name": "Pork-Free Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Lifestyle"],
  },
  "mediterranean_diet": {
    "frontend_name": "Mediterranean Cuisine Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Cultural"],
  },
  "low_fodmap": {
    "frontend_name": "Fermentable-Free FODMAP Friendly Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "beverage_enchancers": {
    "frontend_name": "Beverage Enhancer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Beverage"],
  },
  "convenient_breakfast": {
    "frontend_name": "Convenient Breakfast Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Food"],
  },
  "energy_beveragist": {
    "frontend_name": "Energy Beverage Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Beverage"],
  },
  "asian_cuisine": {
    "frontend_name": "Asian Cuisine Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Cultural"],
  },
  "pizza_meal_households": {
    "frontend_name": "Pizza Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Food"],
  },
  "natural_beveragist": {
    "frontend_name": "Natural Beverage Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Beverage"],
  },
  "fruit_and_natural_snackers": {
    "frontend_name": "Fruit Snack Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Food"],
  },
  "isotonic_beveragist": {
    "frontend_name": "Isotonic Beverage Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Beverage"],
  },
  "meat_snackers": {
    "frontend_name": "Protein Snack Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Food"],
  },
  "fitness_enthusiast": {
    "frontend_name": "Fitness Enthusiasts",
    "segment_type": "Household",
    "propensity_compisition": ["H"],
    "tags": ["Lifestyle"],
  },
  "casual_auto_fixers": {
    "frontend_name": "Casual Automobile Fixers",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M"],
    "tags": ["Home & Auto"],
  },
  "summer_bbq": {
    "frontend_name": "Grilling Enthusiasts",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "artsy_folk": {
    "frontend_name": "Arts & Crafts Enthusiasts",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M", "L"],
    "tags": ["Lifestyle"],
  },
  "camper": {
    "frontend_name": "Camping Enthusiasts",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M", "L"],
    "tags": ["Lifestyle"],
  },
  "indoor_decor_and_gardening": {
    "frontend_name": "Indoor Gardening Buyers",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M"],
    "tags": ["Home & Auto"],
  },
  "outdoor_decor_and_gardening": {
    "frontend_name": "Outdoor Gardening Buyers",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M"],
    "tags": ["Home & Auto"],
  },
  "reader": {
    "frontend_name": "Reading Enthusiasts",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "gasoline": {
    "frontend_name": "High Volume Fuel Fillers",
    "segment_type": "Household",
    "propensity_compisition": ["H"],
    "tags": ["Fuel"],
  },
  "gasoline_premium_unleaded": {
    "frontend_name": "Premium Unleaded Fuel Buyers",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M"],
    "tags": ["Fuel"],
  },
  "gasoline_reg_unleaded": {
    "frontend_name": "Regular Unleaded Fuel Buyers",
    "segment_type": "Household",
    "propensity_compisition": ["H"],
    "tags": ["Fuel"],
  },
  "gasoline_unleaded_plus": {
    "frontend_name": "Unleaded Plus Fuel Buyers",
    "segment_type": "Household",
    "propensity_compisition": ["H", "M"],
    "tags": ["Fuel"],
  },
  "christmas": {
    "frontend_name": "Winter Holiday Enthusiasts",
    "segment_type": "Seasonal",
    "propensity_compisition": ["H", "M", "L"],
    "tags": ["Seasonal"],
  },
  "easter": {
    "frontend_name": "Spring Holiday Enthusiasts",
    "segment_type": "Seasonal",
    "propensity_compisition": ["H", "M", "L"],
    "tags": ["Seasonal"],
  },
  "halloweeners": {
    "frontend_name": "Halloween Enthusiasts",
    "segment_type": "Seasonal",
    "propensity_compisition": ["H", "M", "L"],
    "tags": ["Seasonal"],
  },
  "back-to-school": {
    "frontend_name": "Back-to-School Product Shoppers",
    "segment_type": "Event",
    "propensity_compisition": ["H", "M"],
    "tags": ["Seasonal"],
  },
  "roadies": {
    "frontend_name": "Road Trippers",
    "segment_type": "Event",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "travelers": {
    "frontend_name": "Travel Enthusiasts",
    "segment_type": "Event",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "metropolitan": {
    "frontend_name": "Metropolitans",
    "segment_type": "Geo-Specific",
    "propensity_compisition": ['H'],
    "tags": ["Lifestyle"],
  },
  "micropolitan": {
    "frontend_name": "Micropolitans",
    "segment_type": "Geo-Specific",
    "propensity_compisition": ['H'],
    "tags": ["Lifestyle"],
  },
  "beautists": {
    "frontend_name": "Beauty Product Buyers",
    "segment_type": "Beauty/Personal Care",
    "propensity_compisition": ['H'],
    "tags": ["Personal Care"],
  },
  "houses_with_children": {
    "frontend_name": "Children Product Buyers",
    "segment_type": "Life Stage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "metabolic": {
    "frontend_name": "Metabolic Product Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "scotch": {
    "frontend_name": "Scotch Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "vodka": {
    "frontend_name": "Vodka Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "rum": {
    "frontend_name": "Rum Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "tequila": {
    "frontend_name": "Tequila Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "gin": {
    "frontend_name": "Gin Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "cognac": {
    "frontend_name": "Cognac Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "brandy": {
    "frontend_name": "Brandy Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "bourbon": {
    "frontend_name": "Bourbon Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "whiskey": {
    "frontend_name": "Whiskey Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "liquor": {
    "frontend_name": "Liquor Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "ayervedic": {
    "frontend_name": "Ayurvedic Cuisine Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "low_calorie": {
    "frontend_name": "Low-Calorie Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Lifestyle"],
  },
  "engine_2": {
    "frontend_name": "Nutrient-Rich Enthusiasts",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "glycemic": {
    "frontend_name": "Low Glycemic Lifestyle Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "plant_based_whole_foods": {
    "frontend_name": "Plant-Powered Whole Foods Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Lifestyle"],
  },
  "ovo-vegetarians": {
    "frontend_name": "Ovo-Vegetarian Pantry Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Lifestyle"],
  },
  "without_beef": {
    "frontend_name": "Meat-Free Grocery Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Lifestyle"],
  },
  "plant_based": {
    "frontend_name": "Veggie-Focused Food Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H", "M"],
    "tags": ["Food"],
  },
  "domestic_beer": {
    "frontend_name": "Domestic Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "domestic_below_premium_beer": {
    "frontend_name": "Domestic Sub Premium Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "domestic_premium_beer": {
    "frontend_name": "Domestic Premium Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "domestic_above_premium_beer": {
    "frontend_name": "Domestic Premium Plus Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "imported_beer": {
    "frontend_name": "Imported Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "imported_asian_beer": {
    "frontend_name": "Imported Asian Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "imported_canadian_beer": {
    "frontend_name": "Imported Canadian Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "imported_european_beer": {
    "frontend_name": "Imported European Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "imported_hispanic_beer": {
    "frontend_name": "Imported Hispanic Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "craft_beer": {
    "frontend_name": "Craft Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "craft_national_beer": {
    "frontend_name": "National Craft Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "craft_local_beer": {
    "frontend_name": "Local Craft Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "beer": {
    "frontend_name": "Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "fmb_and_hard_seltzers": {
    "frontend_name": "Hard Seltzer & Malt Beverage Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "hard_cider": {
    "frontend_name": "Hard Cider Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "non_alcoholic_beverage": {
    "frontend_name": "Non-Alcoholic Beverage Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "non_alcoholic_beer": {
    "frontend_name": "Non-Alcoholic Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "non_alcoholic_wine": {
    "frontend_name": "Non-Alcoholic Wine Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "non_alcoholic_spirits": {
    "frontend_name": "Non-Alcoholic Liquor Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "ready_to_drink": {
    "frontend_name": "Ready to Drink Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "wine": {
    "frontend_name": "Wine Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "red_wine": {
    "frontend_name": "Red Wine Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "rose_wine": {
    "frontend_name": "Rosé Wine Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "sparkling_wine": {
    "frontend_name": "Sparkling Wine Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "white_wine": {
    "frontend_name": "White Wine Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "hard_lemonade": {
    "frontend_name": "Hard Lemonade Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "hard_iced_tea": {
    "frontend_name": "Hard Iced Tea Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "prosecco": {
    "frontend_name": "Prosecco Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "champagne": {
    "frontend_name": "Champagne Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "wine_spritzer": {
    "frontend_name": "Wine Spritzer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "ale": {
    "frontend_name": "Ale Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "stout": {
    "frontend_name": "Stout Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "porter": {
    "frontend_name": "Porter Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "ipa": {
    "frontend_name": "IPA Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "hazy": {
    "frontend_name": "Hazy Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "wheat_beer": {
    "frontend_name": "Wheat Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "pale_ale": {
    "frontend_name": "Pale Ale Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "amber_ale": {
    "frontend_name": "Amber Ale Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "irish_ale": {
    "frontend_name": "Irish Ale Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "blonde_ale": {
    "frontend_name": "Blonde Ale Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "lager": {
    "frontend_name": "Lager Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "pale_lager": {
    "frontend_name": "Pale Lager Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "light_lager": {
    "frontend_name": "Light Lager Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "amber_lager": {
    "frontend_name": "Amber Lager Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "pilsner": {
    "frontend_name": "Pilsner Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "bock": {
    "frontend_name": "Bock Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "oktoberfest": {
    "frontend_name": "Oktoberfest Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "dunkel": {
    "frontend_name": "Dunkel Beer Buyers",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "boomers": {
    "frontend_name": "Baby Boomer Shoppers",
    "segment_type": "Life Stage",
    "propensity_compisition": ["H"],
    "tags": ["Demographic"],
  },
  "gen_x": {
    "frontend_name": "Gen X Shoppers",
    "segment_type": "Life Stage",
    "propensity_compisition": ["H"],
    "tags": ["Demographic"],
  },
  "millennials": {
    "frontend_name": "Millennial Shoppers",
    "segment_type": "Life Stage",
    "propensity_compisition": ["H"],
    "tags": ["Demographic"],
  },
  "gen_z": {
    "frontend_name": "Gen X Shoppers",
    "segment_type": "Life Stage",
    "propensity_compisition": ["H"],
    "tags": ["Demographic"],
  },
  "food": {
    "frontend_name": "PLACEHOLDER FOR food",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Food"],
  },
  "grocery": {
    "frontend_name": "PLACEHOLDER FOR grocery",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Food"],
  },
  "produce": {
    "frontend_name": "PLACEHOLDER FOR produce",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Food"],
  },
  "meat": {
    "frontend_name": "PLACEHOLDER FOR meat",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Food"],
  },
  "deli_and_bakery": {
    "frontend_name": "PLACEHOLDER FOR deli_and_bakery",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Food"],
  },
  "cabernet": {
    "frontend_name": "PLACEHOLDER FOR cabernet",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "pinot_noir": {
    "frontend_name": "PLACEHOLDER FOR pinot_noir",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "merlot": {
    "frontend_name": "PLACEHOLDER FOR merlot",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "malbec": {
    "frontend_name": "PLACEHOLDER FOR malbec",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "port": {
    "frontend_name": "PLACEHOLDER FOR port",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "riesling": {
    "frontend_name": "PLACEHOLDER FOR riesling",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "moscato": {
    "frontend_name": "PLACEHOLDER FOR moscato",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "white_zinfandel": {
    "frontend_name": "PLACEHOLDER FOR white_zinfandel",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "sauvignon_blanc": {
    "frontend_name": "PLACEHOLDER FOR sauvignon_blanc",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "chardonnay": {
    "frontend_name": "PLACEHOLDER FOR chardonnay",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "pinot_grigio": {
    "frontend_name": "PLACEHOLDER FOR pinot_grigio",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
  },
  "bordeaux": {
    "frontend_name": "PLACEHOLDER FOR bordeaux",
    "segment_type": "Food & Beverage",
    "propensity_compisition": ["H"],
    "tags": ["Alcohol"],
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

  elif segmentation_name in segmentations.regex_segmentations:
    segmentation_type = "sensitive"

  elif segmentation_name in segmentations.generation_segmentations:
    segmentation_type = "generations"

  elif segmentation_name in segmentations.department_segmentations:
    segmentation_type = "department"
    
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

  elif segmentation_type == "generations":
    segmentation_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/aiq_generations/{}/".format(segmentation_name)

  elif segmentation_type == "department":
    segmentation_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/department_segmentations/{}/".format(segmentation_name)
  
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

  elif segmentation_type in ["percentile", "fuel", "geospatial", "sensitive", "generations", "department"]:
    files = dbutils.fs.ls(segmentation_dir)
    files = [x[1] for x in files if "{}_".format(segmentation_name) in x[1]]

  files.sort()
  return(files)

def get_upc_type(segmentation_name):
  """
  """
  search_keys = list(em_dict.keys())
  comm_keys = list(cs_dict.keys())
  regex_keys = list(reg_dict.keys())
  dept_keys = ["food", "grocery", "produce", "meat", "deli_and_bakery"]

  if (segmentation_name in search_keys) and (segmentation_name in comm_keys):
    message = (
      "{} is in both of the control dictionaries for ...."
    )
    raise ValueError(message)
  elif segmentation_name in search_keys:
    upc_type = "search_embedding"
  elif segmentation_name in comm_keys:
    upc_type = "commodities_subcommodities"
  elif segmentation_name in regex_keys:
    upc_type = "regex"
  elif segmentation_name in dept_keys:
    upc_type = "department"
  else:
    upc_type = None

  return(upc_type)


def get_upc_directory(segmentation_name):
  """
  """
  upc_type = get_upc_type(segmentation_name)

  if upc_type == "search_embedding":
    upc_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/embedded_dimensions/diet_query_embeddings/"
  elif (upc_type == "commodities_subcommodities") or (upc_type == "regex") or (upc_type == "department"):
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
    
  elif (upc_type == "commodities_subcommodities") or (upc_type == "regex")or (upc_type == "department"):
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
    self.tags = audience_dict[segmentation_name]["tags"]
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