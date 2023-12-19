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
  'asian_cuisine': {
    'frontend_name': '',
    'commodities': [
      'ASIAN FOODS', 'FROZEN AUTHENTIC ASIAN', 'FROZEN AUTHENTIC INDIAN',
      'INDIAN FOODS', 'NF NATURAL ASIAN FOODS', 'REFRIGERATED ASIAN',
    ],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'fitness_enthusiast': {
    'frontend_name': '',
    'commodities': [
      'ATHLETICS', 'EXERCISE', 'SPORTS NUTRITION',
      'WOMENS ATHLETIC', 'JR SPORTWEAR TOPS', 'CHILDRENS ATHLETIC',
    ],
    'sub_commodities': [
      'PERSONALITY/LIFESTYLE - MAGAZINES',
      'PERSONALITY/LIFESTYLE - MAGAZI',
      'MENS-MAGAZINE',
      'WOMENS - MAGAZINE'
    ],
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
  'baker': {
    'frontend_name': '',
    'commodities': [
      'BAKERY PARTY TRAYS', 'BAKEWARE', 'BAKING MIXES',
      'BAKING NEEDS', 'NF BAKING NEEDS',
    ],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'outdoor_décor_and_gardening': {
    'frontend_name': '',
    'commodities': [
      'BIRD/SQRL', 'DECORATIVE GARDEN', 'FERNS (OUTDOOR)', 'FERTILIZERS',
      'FOLIAGE (OUTDOOR)', 'GARDEN BAGGED GOODS', 'HANGING BASKETS (OUTDOOR)',
      'HARD GOODS (OUTDOOR)', 'HARDY MUMS (OUTDOOR)', 'OUTDOOR LIVING',
      'PERENNIALS (OUTDOOR)', 'PLANT CARE&SOIL (OUTDOOR)', 'PLANTERS/ACCESSORIES',
      'SEED/BULBS (OUTDOOR)', 'TREES/SHRUBS (OUTDOOR)', 'WATERING', 'YARD DECOR',
      'ANNUALS (OUTDOOR)',
      ],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'indoor_décor_and_gardening': {
    'frontend_name': '',
    'commodities': [
      'EASTER LILY', 'FLORAL - FOLIAGE PLANTS', 'FLORAL - FLOWING PLANTS',
      'ORCHIDS POTTED', 'POINSETTIA', 'POTTED BULBS (INDOOR)',
    ],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'reader': {
    'frontend_name': '',
    'commodities': [
      'BOOKSTORE', 'MAGAZINES', 'NEWSPAPER',
    ],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'camper': {
    'frontend_name': '',
    'commodities': [
      'CAMPING', 'FISHING', 'HUNTING',
    ],
    'sub_commodities': ['OUTDOORS-MAGAZINE'],
    'weeks': 52,
    'live': False
  },
  'summer_bbq': {
    'frontend_name': '',
    'commodities': ['GRILLS'],
    'sub_commodities': ['CHARCOAL'],
    'weeks': 52,
    'live': False
  },
  'christmas': {
    'frontend_name': '',
    'commodities': [
      'CHRISTMAS', 'CHRISTMAS TREES (OUTDOOR)', 'CHRISTMAS GREENS (OUTDOOR)',
    ],
    'sub_commodities': ['CHRISTMAS PLUSH'],
    'weeks': 52,
    'live': False,
  },
  'smokers': {
    'frontend_name': '',
    'commodities': [
      'CIGARETTES', 'CIGARS', 'ELECTRONIC CIGARETTES',
      'LEAF TOBACCO', 'LITTLE CIGARS', 'MOIST SNUFF', 
      'PIPE TOBACCO', 'SMOKING TOBACCO'
    ],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'artsy_folk': {
    'frontend_name': '',
    'commodities': ['CRAFTS/NOTIONS/YARN'],
    'sub_commodities': ['RECREATION/HOBBY - MAGAZINES', 'HOME/CRAFTS - MAGAZINES'],
    'weeks': 52,
    'live': False,
  },
  'halloweeners': {
    'frontend_name': '',
    'commodities': ['HALLOWEEN'],
    'sub_commodities': ['HALLOWEEN PLUSH'],
    'weeks': 52,
    'live': False,
    },
  'bug_phobes': {
    'frontend_name': '',
    'commodities': ['INSECTICIDES'],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'back-to-school': {
    'frontend_name': '',
    'commodities': ['SCHOOL'],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'sexual_health': {
    'frontend_name': '',
    'commodities': ['SEXUAL HEALTH & WELLNESS'],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'easter': {
    'frontend_name': '',
    'commodities': ['EASTER'],
    'sub_commodities': ['EASTER PLUSH'],
    'weeks': 52,
    'live': False,
  },
  'pizza_meal_households': {
    'frontend_name': '',
    'commodities': [
      'FROZEN PIZZA', 'NF FROZEN PIZZA', 'READY TO HEAT PIZZA',
      'NF PIZZA SAUCE/CRUST MIX', 'PIZZA SAUCE/CRUST/MIX',
    ],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'convenient_breakfast': {
    'frontend_name': '',
    'commodities': ['CNV BREAKFAST&WHOLESOME SNKS', 'NF CEREAL BARS'],
    'sub_commodities': ['INSTANT BREAKFAST', 'INSTANT OATMEAL'],
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
  'meat_snackers': {
    'frontend_name': '',
    'commodities': ['MEAT SNACKS', 'SNACK MEAT'],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'fruit_and_natural_snackers': {
    'frontend_name': '',
    'commodities': ['FRUIT SNACKS', 'NF FRUIT SNACKS', 'NF PACKAGED NATURAL SNACK', 'DRIED FRUIT'],
    'sub_commodities': ['DRIED FRUIT'],
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
  'natural_beveragist': {
    'frontend_name': '',
    'commodities': ['NF REFRIG FUNCTIONAL BEV'],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'beverage_enchancers': {
    'frontend_name': '',
    'commodities': ['BEVERAGE ENHANCERS'],
    'sub_commodities': [],
    'weeks': 52,
    'live': False
  },
  'energy_beveragist': {
    'frontend_name': '',
    'commodities': ['ENERGY DRINKS', 'NF ENERGY DRINKS'],
    'sub_commodities': ['ENERGY DRINKS'],
    'weeks': 52,
    'live': False,
  },
  'juicing_beveragust': {
    'frontend_name': '',
    'commodities': [
      'FROZEN JUICE', 'REFRGRATD JUICES/DRINKS',
      'SHELF STABLE JUICE', 'NF JUICE',
    ],
    'sub_commodities': [],
    'weeks': 52,
    'live': False,
  },
  'isotonic_beveragist': {
    'frontend_name': '',
    'commodities': ['ISOTONIC DRINKS'],
    'sub_commodities': ['FITNESS ISOTONIC DRINKS'],
    'weeks': 52,
    'live': False,
  },
  'houses_with_children': {
    'frontend_name': '',
    'commodities': [
      'CHILDRENS ATHLETIC', 'CHILDRENS CASUALS',
      'KIDS BATH SHOP', 'SEASONAL KIDS',
    ],
    'sub_commodities': [
      'CHILDRENS LOW END', 'CHILDRENS QUALITY', 'CHILDRENS VALUE', 'CHILDREN S ACTIVITY',
      "CHILDREN'S ACTIVITY", 'CHILDRENS RAINBOOTS', 'CHILDRENS SANDAL 11-4',
      'CHILDRENS NIKE', 'CHILDRENS UA', 'CHILDRENS SKECHERS', 'NUT SUPP-CHILDREN',
      'TEENS/CHILDREN - MAGAZINES', 'KIDS MILK DRINKS-ASEPTIC', 'FZ KIDS MEAL',
      'KIDS', 'KIDS FLIP FLOPS', 'KIDS BEDDINGS', 'SUNGLASSES KIDS',
      'KIDS YOGURT', 'NAT/ORGNC KIDS YOGURT',
    ],
    'weeks': 52,
    'live': False,
  },
  'houses_with_toddlers': {
    'frontend_name': '',
    'commodities': ['TODDLER/BOYS', 'TODDLER/GIRLS'],
    'sub_commodities': [
      'TRAINING PANTS', 'YOUTH PANTS',
      'TODDLER BOY SLEEPWEAR', 'TODDLER GIRL SLEEPWEAR', 'TODDLER WIPES',
    ],
    'weeks': 52,
    'live': False,
  },
  'beautists': {
    'frontend_name': '',
    'commodities': [
      'BEAUTY MINIS', 'MULTI-CULTURAL HEALTH & BEAUTY', 'NF COSMETICS',
      'NAILCARE/LASH/COTTON', 'COLOR COSMETICS', 'HAIR COLOR AND DEVELOPERS',
    ],
    'sub_commodities': ['BEAUTY SUPPLEMENTS', 'NF COLLAGEN/BEAUTY'],
    'weeks': 52,
    'live': False,
  },
  'geriatric': {
    'frontend_name': '',
    'commodities': ['ADULT INCONTINENCE', 'ADULT NUTRITION'],
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
  'casual_auto_fixers': {
    'frontend_name': '',
    'commodities': ['AUTO ACCESSORIES', 'AUTO CHEMICALS'],
    'sub_commodities': ['AUTO BODY PAINT/ADHESIVES', 'BATTERIES', 'ELECTRICAL ITEMS', 'GAS CANS AND FUNNELS', 'OIL FILTERS AND ACCESSORIES', 'ANTIFREEZE', 'APPEARANCE CHEMICALS', 'CARB/BRAKE FLUIDS/LUBS', 'FUNCTIONAL FLUIDS', 'LUBRICANTS/CLEANERS', 'RADIATOR ADDITIVES', 'WINDSHIELD'],
    'weeks': 52,
    'live': False,
  }
}

#Segmentations that go through percentile propensity ranking instead of fun-lo propensity ranking
percentile_segmentations = [
  "fitness_enthusiast", "beautists", "beverage_enchancers", "back-to-school",
  "summer_bbq", "convenient_breakfast", "energy_beveragist", "indoor_décor_and_gardening",
  'houses_with_children', 'fruit_and_natural_snackers', 'isotonic_beveragist', 'meat_snackers',
  'natural_beveragist', 'outdoor_décor_and_gardening', 'pizza_meal_households', 'reader',
  'asian_cuisine', 'artsy_folk', 'camper',
  'halloweeners', 'christmas', 'easter', 'casual_auto_fixers',
]
