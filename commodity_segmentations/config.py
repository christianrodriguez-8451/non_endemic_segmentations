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
      'ATHLETICS', 'EXERCISE', 'NF SPORTS NUTRITION',
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
  'outdoor_decor_and_gardening': {
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
  'indoor_decor_and_gardening': {
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
      'KIDS YOGURT', 'NAT/ORGNC KIDS YOGURT', 'KIDS CEREAL',
      'R/C VEHICLES', 'LEGO', 'MALE ACTION', 'ACTION FIGURES', 'NERF & ACTIVITY',
      'BABY DOLLS', 'BARBIE DOLLS', 'FASHION DOLLS',
      'MODELS',
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
sensitive_segmentations = {
  "scotch": {
    "commodities": ["SCOTCH"],
    "sub_commodities": [],
    "weeks": 26
  },
  "vodka": {
    "commodities": ["VODKA"],
    "sub_commodities": [],
    "weeks": 26
  },
  "rum": {
    "commodities": ["RUM"],
    "sub_commodities": [],
    "weeks": 26
  },
  "tequila": {
    "commodities": ["TEQUILA"],
    "sub_commodities": [],
    "weeks": 26
  },  
  "gin": {
    "commodities": ["GIN"],
    "sub_commodities": [],
    "weeks": 26
  },
  "cognac": {
    "commodities": [],
    "sub_commodities": ["COGNAC"],
    "weeks": 26
  },
  "brandy": {
    "commodities": [],
    "sub_commodities": ["BRANDY"],
    "weeks": 26
  },
  "bourbon": {
    "commodities": [],
    "sub_commodities": [
      "BOURBON/TN WHISKEY",
      "BOURBON/TN WHISKEY (42 UNDER PROOF)", 
      "BOURBON/TN WHISKEY (42 UNDER P"
    ],
    "weeks": 26
  },
  "whiskey": {
    "commodities": ["N. AMER WHISKEY"],
    "sub_commodities": ["IRISH WHISKEY"],
    "weeks": 26
  },
  "liquor": {
    "commodities": [
      "SCOTCH", "RUM",
      "VODKA", "TEQUILA",
      "GIN", "BRANDY",
      "N. AMER WHISKEY", "LIQUOR"
    ],
    "sub_commodities": [],
    "weeks": 26
  },
  "beer": {
    "commodities": [
      "DOMESTIC BEER", "DOMESTIC BELOW PREMIUM", "DOMESTIC PREMIUM", "DOMESTIC ABOVE PREMIUM",
      "IMPORTED BEERS", "CRAFT/MICRO BEERS", "BEER OTHER",
    ],
    "sub_commodities": [],
    "weeks": 26
  },
  "domestic_beer": {
    "commodities": [
      "DOMESTIC BEER",
      "DOMESTIC BELOW PREMIUM",
      "DOMESTIC PREMIUM",
      "DOMESTIC ABOVE PREMIUM",
    ],
    "sub_commodities": [],
    "weeks": 26
  },
  "domestic_below_premium_beer": {
    "commodities": ["DOMESTIC BELOW PREMIUM"],
    "sub_commodities": [],
    "weeks": 26
  },
  "domestic_premium_beer": {
    "commodities": ["DOMESTIC PREMIUM"],
    "sub_commodities": [],
    "weeks": 26
  },
  "domestic_above_premium_beer": {
    "commodities": ["DOMESTIC ABOVE PREMIUM"],
    "sub_commodities": [],
    "weeks": 26
  },
  "imported_beer": {
    "commodities": ["IMPORTED BEERS"],
    "sub_commodities": [],
    "weeks": 26
  },
  "imported_asian_beer": {
    "commodities": [],
    "sub_commodities": ["ASIAN BEER"],
    "weeks": 26
  },
  "imported_canadian_beer": {
    "commodities": [],
    "sub_commodities": ["CANADIAN BEER"],
    "weeks": 26
  },
  "imported_european_beer": {
    "commodities": [],
    "sub_commodities": ["EUROPEAN", "BELGIUM", "GERMAN BEER", "UK/IRELAND BEER"],
    "weeks": 26
  },
  "imported_hispanic_beer": {
    "commodities": [],
    "sub_commodities": ["HISPANIC BEER"],
    "weeks": 26
  },
  "craft_beer": {
    "commodities": ["CRAFT/MICRO BEERS"],
    "sub_commodities": [],
    "weeks": 26
  },
  "craft_national_beer": {
    "commodities": [],
    "sub_commodities": ["NATIONAL CRAFT"],
    "weeks": 26
  },
  "craft_local_beer": {
    "commodities": [],
    "sub_commodities": ["LOCAL/REGIONAL CRAFT"],
    "weeks": 26
  },
  "fmb_and_hard_seltzers": {
    "commodities": [],
    "sub_commodities": ["FMB FLAVORS", "HARD SELTZER/WATER"],
    "weeks": 26
  },
  "non_alcoholic_beer": {
    "commodities": ["NON-ALCOHOLIC"],
    "sub_commodities": [],
    "weeks": 26
  },
  "non_alcoholic_wine": {
    "commodities": [],
    "sub_commodities": ["NON ALCOHOLIC"],
    "weeks": 26
  },
  "non_alcoholic_spirits": {
    "commodities": [],
    "sub_commodities": ["SPIRIT NON ALCOHOL"],
    "weeks": 26
  },
  "non_alcoholic_beverage": {
    "commodities": ["NON-ALCOHOLIC"],
    "sub_commodities": ["NON ALCOHOLIC", "SPIRIT NON ALCOHOL"],
    "weeks": 26
  },
  "hard_cider": {
    "commodities": ["CIDER"],
    "sub_commodities": [],
    "weeks": 26
  },
  "ready_to_drink": {
    "commodities": ["RTD COCKTAILS"],
    "sub_commodities": [],
    "weeks": 26
  },
  "red_wine": {
    "commodities": [],
    "sub_commodities": [
      "750ML RED VALUE WINES $0-$7.99",
      "750ML RED PREM WINES $8-14.99",
      "750ML RED LUXURY WINES ABOVE $",
      "750ML RED LUXURY WINES ABOVE $15",
    ],
    "weeks": 26
  },
  "white_wine": {
    "commodities": [],
    "sub_commodities": [
      "750ML WHITE VALUE WINES $0-$7.", "750ML WHITE VALUE WINES $0-$7.99",
      "750ML WHITE PREM WINES $8-14.9", "750ML WHITE PREM WINES $8-14.99",
      "750ML WHITE LUXURY WINES ABOVE", "750ML WHITE LUXURY WINES ABOVE $15",
    ],
    "weeks": 26
  },
  "sparkling_wine": {
    "commodities": ["SPARKLING WINE"],
    "sub_commodities": ["SPARKLING WINES"],
    "weeks": 26
  },
  "rose_wine": {
    "commodities": [],
    "sub_commodities": [
      "750ML ROSE/BLUSH VALUE WINES $", "750ML ROSE/BLUSH VALUE WINES $0-$7.99",
      "750ML ROSE/BLUSH PREM WINES AB", "750ML ROSE/BLUSH PREM WINES ABOVE $8",
    ],
    "weeks": 26
  },
  "wine": {
    "commodities": ["TABLE WINE", "MISC WINE", "SPARKLING WINE"],
    "sub_commodities": [],
    "weeks": 26
  },
}

regex_segmentations = {
  #Hard bevs
  "hard_lemonade": {
    "filter": {"micro_department": ["BEER", "SPIRITS"]},
    "must_contain": [["lemonade"]],
  },
  "hard_iced_tea": {
    "filter": {"sub_department": ["LIQUOR"]},
    "must_contain": [[" tea"]],
  },
  #Wine
  "prosecco": {
    "filter": {"micro_department": ["WINE"]},
    "must_contain": [["prosecco"]],
  },
  "champagne": {
    "filter": {"micro_department": ["WINE"]},
    "must_contain": [["champagne"]],
  },
  "wine_spritzer": {
    "filter": {"micro_department": ["WINE"]},
    "must_contain": [["spritzer"]],
  },
  #Ales
  "ale": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [[" ale"]],
    "composes": "ale",
  },
  "stout": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["stout"]],
    "composes": "ale",
  },
  "porter": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["porter"]],
    "composes": "ale",
  },
  "ipa": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["ipa", "indian pale", "india pale"]],
    "composes": "ale",
  },
  "hazy": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["hazy", "new england ipa", "new england indian pale ale", "new england india pale ale"]],
    "composes": "ale",
  },
  "wheat_beer": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["wheat"], ["beer", " ale"]],
    "composes": "ale",
  },
  "pale_ale": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["pale"], [" ale"]],
    "must_not_contain": [["ipa", "indian", "india"]],
    "composes": "ale",
  },
  "amber_ale": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["amber"], [" ale"]],
    "composes": "ale",
  },
  "irish_ale": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["irish"], [" ale"]],
    "composes": "ale",
  },
  "blonde_ale": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["blonde", "blond"], [" ale"]],
    "composes": "ale",
  },
  #Lagers
  "lager": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["lager"]],
    "composes": "lager",
  },
  "pale_lager": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["pale"], ["lager"]],
    "composes": "lager",
  },
  "light_lager": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["light", "lite"], ["lager"]],
    "composes": "lager",
  },
  "amber_lager": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["amber"], ["lager"]],
    "composes": "lager",
  },
  "pilsner": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["pilsner"]],
    "composes": "lager",
  },
  "oktoberfest": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["okto"]],
    "composes": "lager",
  },
  "bock": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["bock"]],
    "composes": "lager",
  },
  "dunkel": {
    "filter": {"micro_department": ["BEER"]},
    "must_contain": [["dunkel"]],
    "composes": "lager",
  },
}

#Segmentations that go through percentile propensity ranking instead of fun-lo propensity ranking
percentile_segmentations = [
  "fitness_enthusiast", "beautists", "beverage_enchancers", "back-to-school",
  "summer_bbq", "convenient_breakfast", "energy_beveragist", "indoor_decor_and_gardening",
  'houses_with_children', 'fruit_and_natural_snackers', 'isotonic_beveragist', 'meat_snackers',
  'natural_beveragist', 'outdoor_decor_and_gardening', 'pizza_meal_households', 'reader',
  'asian_cuisine', 'artsy_folk', 'camper',
  'halloweeners', 'christmas', 'easter', 'casual_auto_fixers',
]
