# Databricks notebook source
"""
Creates the mongDB json files given the segmentation's backend_name,
frontend_name, description, and groupName. Also creates the on-premise
json files given the segmentation's backend_name, frontend_name, description,
segmentationId, and selectedValues.

To do:
  1) Re-factor the functions so that the mongoDB and on-premise json files
  are created in a single pass. Do this by making segmentationId an argument
  for mongodb_template.

  2) Write out the json files using PySpark (Steven Martz's code).

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

def draw_id(taken_ids = []):
  """
  Randomly draws a UUID in string format.
  """
  id_str = "token"
  taken_ids += ["token"]

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
  groupName="Food & Beverage",
  ):
  """
  Creates an audience template for the mongoDB side.
  """
  backend_name = backend_name.lower().strip()
  id_str = draw_id()
  #my_uuid = uuid.UUID(id_str)
  #uuid_buffer = my_uuid.bytes
  #uuid_str = str(uuid_buffer).replace('-', '')

  #Template from engineering
  template = {
    "_id": {"$uuid": id_str.replace("-", "")},
    "groupName": groupName,
    "name": backend_name.upper().strip(),
    "label": frontend_name.strip(),
    "description": description.strip(),
    "columnName": "segment",
    "fileName": f"/data/mart/comms/prd/krogerSegments/{backend_name}/",
    "fileType": "PARQUET",
    "fileMask": f"{backend_name}_",
    "fileDate": "LATEST",
    "_class": "PredefinedSegmentation",
    "mutuallyExclusive": False,
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
    ],
    "filterType": "IN_LIST",
    "isInternal": False,
    "ehhnColumnName": "ehhn"
  }
  return(template)

def onprem_template(
    backend_name,
    frontend_name="MARKETING - INSERT FINAL NAME HERE",
    description="MARKETING - INSERT FINAL DESCRIPTION HERE",
    segmentationId = "COPY & PASTE SEGMENTATION ID HERE",
    selectedValues=["H"],
    ):
    """
    Creates an audience template for the on-premise side.
    """
    template = {
    "name": frontend_name,
    "description": description,
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


#groupName
#backend_name
#frontend_name (not needed)
#description (not needed)
#selectedValues

# COMMAND ----------

#Example Usage
groupName = "Food & Beverage"
backend_name = "breakfast_buyers"
frontend_name = "Breakfast Product Buyers"
description = "Buyers with a high propensity for purchasing breakfast related products from multiple categories including Ref breakfast, breakfast sausage, frozen breakfast, breakfast foods, cold cereals, NF cereals and oatmeal."
template = mongodb_template(backend_name, frontend_name, description, groupName)

print(template)

# COMMAND ----------

#Segments used for batch 1 and batch 2 deployment
segments =[
    ["Food & Beverage", "beveragist", "Beverage Product Buyers", "Buyers with a high propensity for purchasing drinks and beverages specifically, single serve, soft drink and specialty soft drink buyers.", ["H", "M"]],
    ["Food & Beverage", "breakfast_buyers", "Breakfast Product Buyers", "Buyers with a high propensity for purchasing breakfast related products from multiple categories including Ref breakfast, breakfast sausage, frozen breakfast, breakfast foods, cold cereals, NF cereals and oatmeal.", ["H", "M"]],
    ["Food & Beverage", "hispanic_cuisine", "Hispanic Cuisine Product Buyers", "Buyers with a high propensity for purchasing foods in the authentic Central American foods category including frozen Hispanic influenced food, Mexican tortilla, NF Mexican inspired foods, refrigerated Hispanic inspired grocery, traditional Mexican influenced foods.", ["H", "M"]],
    ["Food & Beverage", "low_fodmap", "Fermentable-Free FODMAP Friendly Product Buyers", "Buyers that prefer to purchase products that are high in these fermentable carbohydrates. Common high FODMAP foods include certain fruits (e.g., apples, pears), certain vegetables (e.g., onions, garlic, cauliflower), dairy products containing lactose, wheat-based products, and certain legumes and sweeteners.", ["H"]],
    ["Food & Beverage", "low_protein", "Low Protein Buyers", "Buyers that prefer to purchase products that include foods that are lower in protein content, such as Grains(Rice, pasta, bread, and cereals), Fruits(Apples, bananas, berries, etc.), Vegetables( Broccoli, carrots, spinach, etc.), Fats( Oils, avocados, and olives), Low-protein bread and pasta alternatives (made from non-wheat flours like corn or rice).", ["H", "M"]],
    ["Food & Beverage", "low_salt", "Low Salt Buyers", "Buyers that prefer to avoid purchasing products that are processed and packaged foods(canned soups, sauces, salad dressings, and processed meats like bacon, deli meats), Fast food and restaurant meals, Snack foods( Chips, pretzels, and other salty snacks), Pickled and cured foods(pickles, olives, and cured meats), Condiments( Sauces like soy sauce, ketchup, and some mustard).", ["H", "M"]],
    ["Food & Beverage", "macrobiotic", "Macro Product Buyers", "Buyers that prefer to purchase products that are whole, natural, local, and seasonal foods and are prepared simply like steaming, boiling, and stir-frying. Raw foods are generally minimized. Preference to avoid processed and refined foods, as well as artificial additives and preservatives.", ["H", "M"]],
    ["Food & Beverage", "mediterranean_diet", "Mediterranean Cuisine Product Buyers", "Buyers with a high propensity for purchasing products in the authentic Mediterranean foods category include buying an Abundance of fruits and vegetables, Whole grains(wheat, barley, oats, and brown rice), Healthy fats(Olive oil, Nuts, seeds, and avocados), Lean proteins(Fish and poultry, beans, lentils, chickpeas). Limited Dairy and cheese, sweets and red meat.", ["H"]],
    ["Food & Beverage", "non_veg", "Non-Vegetarian Buyers", "Buyers that prefer to purchase products including: Meat(beef, pork, lamb, chicken, and venison), Poultry(chicken, turkey, duck, and other fowl), Fish and Seafood(salmon, tuna, trout, and seafood like shrimp, prawns, crabs, and mollusks), Eggs, Dairy Products(milk, cheese, yogurt, and butter)", ["H", "M"]],
    ["Food & Beverage", "organic", "Organic Product Buyers", "Buyers that prefer to purchase products with 'organic' commodities labled. Includes milk, certain proteins, organic fruit and vegetables.", ["H", "M"]],
    ["Food & Beverage", "salty_snackers", "Snack Product Buyers", "Buyers that prefer to purchase products in the snacking category.", ["H", "M"]],
    ["Household", "gasoline_premium_unleaded", "Premium Unleaded Fuel Buyers", "Shoppers than have purchased Premium Unleaded Fuel in the last year", ["H", "M"]],
    ["Household", "gasoline_unleaded_plus", "Premium Unleaded Plus Fuel Buyers", "Shoppers than have purchased Unleaded Plus Fuel in the last year", ["H", "M"]],
    ["Household", "gasoline_reg_unleaded", "Regular Unleaded Fuel Buyers", "Shoppers than have purchased Regular Unleaded Fuel in the last year", ["H"]],
]

# COMMAND ----------

#Creates the templates for MongoDB
for s in segments:

    groupName = s[0]
    backend_name = s[1]
    frontend_name = s[2]
    description = s[3]
    template = mongodb_template(backend_name, frontend_name, description, groupName)

    fn = "mongodb_" + backend_name + ".json"
    print(template)
    #with open(fn, "w+") as outfile:
        #json.dump(template, outfile, indent=1)


# COMMAND ----------

for s in segments:

    backend_name = s[1]
    frontend_name = s[2]
    description = s[3]
    selectedValues = s[4]
    template = onprem_template(
        backend_name=backend_name,
        frontend_name=frontend_name,
        description=description,
        selectedValues=selectedValues,
    )

    fn = backend_name + ".json"
    print(template)
    #with open(fn, "w+") as outfile:
    #    json.dump(template, outfile, indent=1)

