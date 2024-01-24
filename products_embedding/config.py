DATE_RANGE_WEIGHTS = {4: 0.27, 3: 0.26, 2: 0.24, 1: 0.23}

# def dummy_fn(x, y):
#   return(x + y)

#Proposal - at storage account sa8451dbxadhocprd and storage container media, let us create
#a new directory called non_endemic_segmentations. Inside of it, we can put commodity_segments and embedded_dimensions.
#That way, all the non-endemic processes live in the save neighborhood.
#Proposal - create prd, tst, and dev directories on Azure to facilitate easy testing + development

#Where the commodity-segment's output lands
output_fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/"

#Where the exploratory household analysis lands
hh_counts_fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/commodity_segments/household_counts/"

#Dictionary used to define each segmentation's commodities, sub-commodities, weeks, and deployment status
commodity_segmentations = {
  'free_from_gluten': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'grain_free': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'healthy_eating': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
    },
  'ketogenic': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'kidney-friendly': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'lactose_free': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'low_bacteria': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'paleo': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False
  },
  'vegan': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False
  },
  'vegetarian': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'hispanic_cuisine': {
    'frontend_name': '',
    'commodities': [
      'AUTH CENTRAL AMER FOODS', 'AUTHENTIC HISPANIC FDS&PRODUCT',
      'FROZEN HISPANIC', 'MEXICAN TORTILLA/WRAP/TOSTADA', 'NF MEXICAN PRODUCTS',
      'REFRIGERATED HISPANIC GROCERY', 'TRADITIONAL MEXICAN FOODS',
    ],
    'sub_commodities': ['HISPANIC BEER'],
    'weeks': 52,
    'live': False,
  },
  'low_fodmap': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'mediterranean_diet': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
    },
  'non_veg': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'low_salt': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'low_protein': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'heart_friendly': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'macrobiotic': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'high_protein': {
    'frontend_name': '',
    'commodities': [''],
    'sub_commodities': [''],
    'weeks': 52,
    'live': False,
  },
  'breakfast_buyers': {
    'frontend_name': '',
    'commodities': [
      'REF BREAKFAST', 'BREAKFAST SAUSAGE', 'NF FROZEN BREAKFAST',
      'FROZEN BREAKFAST FOODS', 'CEREALS', 'COLD CEREAL', 'NF CEREALS',
    ],
    'sub_commodities': ['GRITS', 'STANDARD OATMEAL', 'OTHER HOT CEREAL'],
    'weeks': 52,
    'live': False,
  },
  'salty_snackers': {
    'frontend_name': '',
    'commodities': [
      'BAG SNACKS', 'SNACKS', 'SS/VENDING - SALTY SNACKS',
      'TRAIL MIX & SNACKS', 'WAREHOUSE SNACKS', 'NF SNACK',
    ],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'ayervedic': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'low_calorie': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'beveragist':{
    'frontend_name': '',
    'commodities': ['SINGLE SERVE BEVERAGE', 'SOFT DRINKS', 'SPECIALTY SOFT DRINKS'],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'engine_2': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'glycemic': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False
  },
  'plant_based': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'plant_based_whole_foods': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'pescetarian': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'raw_food': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'without_beef': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'without_pork': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  '': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'organic': {
    'frontend_name': '',
    'commodities': [
      'NF ORGANIC MILK', 'ORGANIC BEEF', 'ORGANIC CHICKEN', 'ORGANIC FRUIT & VEGETABLES',
      'ORGANIC TURKEY', 'PORK ORGANIC',
    ],
    'sub_commodities': [
      'APPLES AMBROSIA ORGANIC', 'APPLES CRIPPS PINK ORGANIC', 'APPLES ENVY ORGANIC',
      'APPLES FUJI (BULK&BAG) ORGANIC', 'APPLES GALA (BULK&BAG) ORGANIC',
      'APPLES GOLD DEL BULK/BAG ORGANIC', 'APPLES GRNY SMTH BULK/BAG ORGANIC',
      'APPLES HONEYCRISP ORGANIC', 'APPLES JAZZ ORGANIC', 'APPLES OTHER (BULK&BAG) ORGANIC',
      'APPLES RED DELICIOUS (BULK&BAG) ORGANIC', 'BACON - NATURAL/ORGANIC',
      'BANANAS ORGANIC', 'BERRIES OTHER ORGANIC', 'BLACKBERRIES ORGANIC',
      'BLUEBERRIES ORGANIC', 'CRANBERRIES ORGANIC', 'RASPBERRIES ORGANIC',
      'STRAWBERRIES ORGANIC', 'BREAD:DIET/ORGANIC', 'BROCCOLI WHOLE & CROWNS ORGANIC',
      'CAULIFLOWER WHOLE ORGANIC', 'DRESSING ORGANICS', 'ORGANIC MILK',
      'FRESH DINNER SAUSAGE NATURAL/ORGANIC', 'FRZN ORGANIC VEGETABLES',
      'FRUIT/VEGETABLE/HERB ORGANIC (', 'FRUIT/VEGETABLE/HERB ORGANIC (OUTDOOR)',
      'FRZN MEAT - NATURAL/ORGANIC', 'CANTALOUPE WHOLE ORGANIC', 'HONEYDEW WHOLE ORGANIC',
      'MELONS WHOLE OTHER ORGANIC', 'WATERMELON PERSONAL ORGANIC', 'WATERMELON SEEDLESS WHOLE ORGANIC',
      'MUSHROOM PORTABELLA ORGANIC', 'MUSHROOMS OTHERS ORGANIC', 'MUSHROOMS WHITE SLICED ORGANIC',
      'MUSHROOMS WHITE WHOLE PKG ORGANIC', 'DRIED FRUIT OTHER ORGANIC',
      'ROLLS: DIET/ORGANIC', 'SALAD BOWLS ORGANIC', 'SALAD MIX BLENDS ORGANIC',
      'SALAD MIX KITS ORGANIC', 'SALAD SPINACH ORGANIC', 'SQUASH OTHER ORGANIC',
      'SALAD BAR ORGANIC', 'DINNER SAUSAGE NATURAL/ORGANIC', 'GARLIC JAR ORGANIC',
      'HERBS DRIED ORGANIC', 'SPICES & SEASONINGS ORGANIC', 'APRICOTS ORGANIC',
      'CHERRIES ORGANIC', 'NECTARINES WHITE FLESH ORGANIC',
      'NECTARINES YELLOW FLESH ORGANIC', 'PEACHES WHITE FLESH ORGANIC',
      'PEACHES YELLOW FLESH ORGANIC', 'PLUMS ORGANIC', 'PLUOTS ORGANIC',
      'STONE FRUIT OTHER ORGANIC', 'TOMATOES CHERRY ORGANIC', 'TOMATOES GRAPE ORGANIC',
      'TOMATOES OTHERS ORGANIC', 'TOMATOES RED HH ORGANIC', 'TOMATOES RED OTV ORGANIC',
      'TOMATOES ROMA ORGANIC', 'AVOCADO ORGANIC', 'KIWI FRUIT ORGANIC',
      'MANGO ORGANIC', 'PINEAPPLE WHOLE&PEEL/CORED ORGANIC', 'POMEGRANATES ORGANIC',
      'TROPICAL FRUIT OTHER ORGANIC', 'ORGANIC', 'ORGANIC FRUIT/VEG INSTORE PROC',
      'ORGANIC FRUIT/VEG INSTORE PROCESSED', 'ASPARAGUS ORGANIC', 'BEANS ORGANIC',
      'CABBAGE ORGANIC', 'CELERY ORGANIC', 'GREENS BULK ORGANIC', 'HARD SQUASH ORGANIC',
      'ORGANIC VEGETABLES ALL OTHERS', 'YELLOW SUMMER SQUASH ORGANIC',
      'ZUCCHINI ORGANIC', 'GREENS PACKAGED ORGANIC', 'ORGANIC VALUE ADDED VEGETABLES',
      'VEGETABLES COOKING PACKAGED ORGANIC', 'CUCUMBERS ORGANIC',
      'GREEN ONIONS ORGANIC', 'HEAD LETTUCE ORGANIC', 'RADISHES ORGANIC',
      'SPINACH ORGANIC', 'VARIETY LETTUCE ORGANIC', 'WET CAT FOOD - ORGANIC/NATURAL',
      'WET DOG FOOD - ORGANIC/NATURAL', 'CARROTS BAGGED ORGANIC',
      'CARROTS BULK ORGANIC', 'CARROTS MINI PEELED ORGANIC', 'CITRUS OTHER ORGANIC',
      'CLEMENTINES ORGANIC', 'GRAPEFRUIT ORGANIC', 'LEMONS ORGANIC', 'LIMES ORGANIC',
      'ORANGES NAVELS ALL ORGANIC', 'ORANGES NON NAVEL ALL ORGANIC',
      'TANGERINES & TANGELOS ORGANIC', 'CORN ORGANIC', 'YARD ORGANICS',
      'GRAPES BLACK/BLUE ORGANIC', 'GRAPES OTHER ORGANIC', 'GRAPES RED GLOBE ORGANIC',
      'GRAPES RED ORGANIC', 'GRAPES WHITE ORGANIC', 'GARLIC WHOLE CLOVES ORGANIC',
      'HERBS BULK ORGANIC', 'HERBS FRESH OTHER ORGANIC', 'HERBS ORGANIC PACKAGED',
      'HERBS PARSLEY ORGANIC', 'JUICE FRT & VEG BLENDS ORGANIC',
      'JUICE FRT & VEG BLENDS ORGANIC (OVER 50% JCE', 'JUICE FRT OR VEG 100% ORGANIC',
      'JUICE FRT OR VEG 100% ORGANIC (OVER 50% JUIC', 'JUICES ORGANIC (OVER 50% JUICE',
      'ORGANIC', 'LUNCHMEAT - NATURAL/ORGANIC', 'ORGANIC MILK', 'NF ORGANIC PASTA SAUCE',
      'ORGANIC COFFEE & TEA', 'ORGANIC JUICE ALL OTHER', 'ORGANIC LEMONADE',
      'ORGANIC ORANGE JUICE', 'ORGANIC SALAD MIX', 'NF ORGANIC SALAD DRESSING',
      'ORGANIC MULTI SERVE/PACK YOGUR', 'ORGANIC MULTI SERVE/PACK YOGURT',
      'ORGANIC SS YOGURT', 'NUTS OTHER ORGANIC', 'ORGANIC OLIVE OIL',
      'ONIONS OTHER ORGANIC', 'ONIONS RED (BULK&BAG) ORGANIC',
      'ONIONS SWEET ORGANIC', 'ONIONS YELLOW (BULK&BAG) ORGANIC',
      'BLUEBERRIES ORGANIC', 'HERBS FRESH OTHER ORGANIC', 'SQUASH OTHER ORGANIC',
      'PEARS ANJOU ORGANIC', 'PEARS ASIAN ORGANIC', 'PEARS BARTLETT ORGANIC',
      'PEARS BOSC ORGANIC', 'PEARS OTHER ORGANIC', 'PEARS RED ORGANIC',
      'PEPPERS ALL OTHERS ORGANIC', 'PEPPERS GREEN BELL ORGANIC',
      'PEPPERS OTHER BELL ORGANIC', 'PEPPERS RED BELL ORGANIC',
      'PEPPERS YELLOW BELL ORGANIC', 'ORGANICS',
      'POTATOES GOURMET ORGANIC', 'POTATOES OTHER ORGANIC',
      'POTATOES RED (BULK&BAG) ORGANIC', 'POTATOES RUSSET (BULK&BAG) ORGANIC',
      'POTATOES SWEET ORGANIC',
    ],
    'weeks': 52,
    'live': False,
  },
  '': {
    'frontend_name': '',
    'commodities': [],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  }
}

# Segmentations that go through fun-lo propensity ranking
funlo_segmentations = [
  "free_from_gluten", "grain_free", "healthy_eating", "ketogenic", "kidney-friendly", "lactose_free", "low_bacteria",
  "paleo", "vegan", "vegetarian", "beveragist", "breakfast_buyers", "hispanic_cuisine", "low_fodmap",
  "mediterranean_diet", "organic", "salty_snackers", "non_veg", "low_salt", "low_protein", "heart_friendly",
  "macrobiotic", "high_protein", "ayervedic", "low_calorie", "engine_2", "glycemic", "plant_based_whole_foods",
  "pescetarian", "raw_food", "without_beef", "without_pork"
]
