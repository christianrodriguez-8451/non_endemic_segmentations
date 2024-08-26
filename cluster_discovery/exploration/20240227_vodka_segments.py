# Databricks notebook source
# MAGIC %md
# MAGIC # Vodka Segment Investigation

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %pip install fuzzywuzzy

# COMMAND ----------

from fuzzywuzzy import fuzz
from functools import reduce
from itertools import combinations
from collections import Counter
import numpy as np
import re

import pyspark.sql.functions as f
from pyspark.sql.types import DoubleType, BooleanType, IntegerType, ArrayType, StringType

from effodata import ACDS, golden_rules

# COMMAND ----------

def match_string(s1, s2):
    val = fuzz.token_sort_ratio(s1, s2)
    return val
    
MatchUDF = udf(match_string, DoubleType())

# COMMAND ----------

acds = ACDS(use_sample_mart=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Exploration

# COMMAND ----------

products_aug_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/cluster_views/products_aug'
products_aug = spark.read.parquet(products_aug_path)
products_aug.limit(20).display()

# COMMAND ----------

# Load PIM
pim_path = "abfss://pim@sa8451posprd.dfs.core.windows.net/pim_core/by_cycle/cycle_date=20240113"
pim = spark.read.parquet(pim_path)
pim.limit(20).display()

# COMMAND ----------

keywords = ['vodka']

fields = [
    'con_dsc_tx', 
    'fyt_pmy_dpt_cct_dsc_tx',
    'fyt_rec_dpt_cct_dsc_tx',
    'fyt_sub_dpt_cct_dsc_tx',
    'fyt_com_cct_dsc_tx',
    'fyt_sub_com_cct_dsc_tx',
    'pim_description',
    'pinto_description'
]

mega_cond = reduce(
    lambda a, b: a | b, 
    [f.lower(f.col(col_name)).contains(keyword)
     for col_name in fields
     for keyword in keywords]
)
keyword_filtered = products_aug.filter(mega_cond).select('bas_con_upc_no', *fields)
print(f'{keyword_filtered.count()} products returned')
keyword_filtered.display()

# COMMAND ----------

(
    keyword_filtered
    .groupBy('fyt_com_cct_dsc_tx')
    .count()
    .display()
)

# COMMAND ----------

all_vodka = products_aug.filter(f.col('fyt_com_cct_dsc_tx') == '260 VODKA')
all_vodka.limit(20).display()

# COMMAND ----------

(
    all_vodka
    .join(
        pim.select(
            f.col('upc_key').alias('con_upc_no'),
            'krogerDerivedNormalizedFlavor', 'krogerOwnedFlavor'
        ).distinct(),
        on='con_upc_no',
        how='left'
    )
    .select(*fields, 'krogerDerivedNormalizedFlavor', 'krogerOwnedFlavor')
    .filter(f.col('krogerDerivedNormalizedFlavor').isNotNull() | (f.length(f.col('krogerOwnedFlavor')) > 0))
    .display()
)

# COMMAND ----------

(
    all_vodka
    .groupBy('fyt_sub_com_cct_dsc_tx')
    .count()
    .display()
)

# COMMAND ----------

(
    all_vodka
    .filter(f.col('fyt_sub_com_cct_dsc_tx') == '36002 TRADITIONAL FLAVORED VODKA')
    .select(*fields)
    .display()
)

# COMMAND ----------

(
    all_vodka
    .filter(f.col('fyt_sub_com_cct_dsc_tx') == '36000 TRADITIONAL VODKA')
    .select(*fields)
    .display()
)

# COMMAND ----------

(
    all_vodka
    .filter(f.col('fyt_sub_com_cct_dsc_tx') == '36004 SMALL SIZE VODKA (375ML&SMALLER)')
    .select(*fields)
    .display()
)

# COMMAND ----------

(
    all_vodka
    .filter(f.lower(f.col('pim_description')).contains('variety'))
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Regex Grouping

# COMMAND ----------

traditional_names = [row.pim_description.lower().strip(') ').strip('(pim) ') for row in 
                     all_vodka.filter(f.col('fyt_sub_com_cct_dsc_tx') == '36000 TRADITIONAL VODKA').collect()]
print(f'{len(traditional_names)} total')
traditional_names

# COMMAND ----------

two_words_expr = re.compile(r"^[a-z'0-9.®]+ vodka$")
two_words = [name for name in traditional_names if two_words_expr.match(name)]
print(f'{len(two_words)} found')
two_words

# COMMAND ----------

not_two_words = [name for name in traditional_names if not two_words_expr.match(name)]
not_two_words

# COMMAND ----------

three_words_expr = re.compile(r"^[a-z'0-9.®]+ [a-z'0-9.®]+ vodka$")
three_words = [name for name in not_two_words if three_words_expr.match(name)]
print(f'{len(three_words)} found')
three_words

# COMMAND ----------

not_three_words = [name for name in not_two_words if not three_words_expr.match(name)]
not_three_words

# COMMAND ----------

general_flavor_keywords = [
    'infused',
    'flavored',
    'flavor',
    'liqueur',
]

specific_flavor_keywords = [
    # Fruits
    'raspberry',
    'strawberry',
    'blueberry',
    'blackberry',
    'cranberry',
    'cherry',
    'peach',
    'pear',
    'apple',
    'pineapple',
    'mango',
    'watermelon',
    'lemon',
    'lime',
    'orange',
    'grapefruit',
    'kiwi',
    'passionfruit',
    'papaya',
    'guava',
    'coconut',
    'lychee',
    'pomegranate',
    'fig',
    'plum',
    'apricot',
    'grape',
    'banana',
    'melon',
    'citron',
    'tangerine',
    'blood orange',
    'nectarine',
    'cranberry',
    'kumquat',
    'persimmon',
    'starfruit',
    'boysenberry',
    'huckleberry',
    'elderberry',
]

flavor_keywords = general_flavor_keywords + specific_flavor_keywords

flavor_names = [name for name in not_three_words if any(keyword in name for keyword in flavor_keywords)]
uncaught = [name for name in not_three_words if not any(keyword in name for keyword in flavor_keywords)]
print(f'{len(flavor_names)} flavored found')
flavor_names

# COMMAND ----------

uncaught

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



all_text = [word
            for row in all_vodka
                       .filter(f.col('fyt_sub_com_cct_dsc_tx') == '36002 TRADITIONAL FLAVORED VODKA')
                       .collect()
            for word in row.pim_description.lower().split()]

word_counts = sorted(Counter(all_text).items(), key=lambda item: -item[1])
word_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Manual Consolidation

# COMMAND ----------

all_text = [word.strip('()')
            for row in all_vodka.collect()
            for word in row.pim_description.lower().split()]

word_counts = sorted(Counter(all_text).items(), key=lambda item: -item[1])
word_counts

# COMMAND ----------

manufacturers = {row.prn_mfg_dsc_tx.lower()
                 for row in all_vodka.collect()}
manufacturers

# COMMAND ----------

def drop_manufacturers(name):
    name = name.lower()
    for mfr in manufacturers:
        if mfr in name:
            name = name.replace(mfr, '')
    return name.split()

all_text = [word.strip('()[]{}™®')
            for row in all_vodka.collect()
            for word in drop_manufacturers(row.pim_description)]
all_text = [word for word in all_text
            if not word.isnumeric()
            and len(word) > 0]

word_counts = sorted(Counter(all_text).items(), key=lambda item: -item[1])
word_counts

# COMMAND ----------

def getRoots(aNeigh):
    def findRoot(aNode,aRoot):
        while aNode != aRoot[aNode][0]:
            aNode = aRoot[aNode][0]
        return (aNode,aRoot[aNode][1])
    myRoot = {} 
    for myNode in aNeigh.keys():
        myRoot[myNode] = (myNode,0)  
    for myI in aNeigh: 
        for myJ in aNeigh[myI]: 
            (myRoot_myI,myDepthMyI) = findRoot(myI,myRoot) 
            (myRoot_myJ,myDepthMyJ) = findRoot(myJ,myRoot) 
            if myRoot_myI != myRoot_myJ: 
                myMin = myRoot_myI
                myMax = myRoot_myJ 
                if  myDepthMyI > myDepthMyJ: 
                    myMin = myRoot_myJ
                    myMax = myRoot_myI
                myRoot[myMax] = (myMax,max(myRoot[myMin][1]+1,myRoot[myMax][1]))
                myRoot[myMin] = (myRoot[myMax][0],-1) 
    myToRet = {}
    for myI in aNeigh: 
        if myRoot[myI][0] == myI:
            myToRet[myI] = []
    for myI in aNeigh: 
        myToRet[findRoot(myI,myRoot)[0]].append(myI) 
    return myToRet


def condense_similar_items(in_list, thresh=85):
    # Get pairs
    all_pairs = [names for names in combinations(in_list, 2)
                 if match_string(*names) >= thresh]

    # Build association list
    all_neighbors = {}
    for p1, p2 in all_pairs:
        all_neighbors.setdefault(p1, set()).update([p2])
        all_neighbors.setdefault(p2, set()).update([p1])

    # Get popularities
    popularities = {k: len(v) for k, v in all_neighbors.items()}

    # Get connected components
    cc = getRoots(all_neighbors)

    # Assign the most popular as the label
    cc_pop = {max(v, key=lambda x: popularities[x]): v
              for v in cc.values()}
    
    # Get the misfits
    all_joined = set().union(*cc_pop.values())
    misfits = set(in_list) - all_joined

    return cc_pop, misfits


def get_condensed_stats(partition, misfits):
    lengths = [len(v) for v in partition.values()]

    print(f'Number of Partitions: {len(partition)}')
    print(f'Longest Partition: {max(lengths)}')
    print(f'Average Partition Length: {sum(lengths)/len(lengths)}')
    print(f'Misfits: {len(misfits)}')

for thresh in [80, 85, 90]:
    print(f'--- {thresh} ---')
    get_condensed_stats(*condense_similar_items([i[0] for i in word_counts], thresh))
    print()

# COMMAND ----------

partitions, misfits = condense_similar_items([i[0] for i in word_counts], 80)
partitions

# COMMAND ----------

misfits

# COMMAND ----------

# Manual definitions
one_offs = [
    'apple',
    'apl',
    'banana',
    'cake',
    'candy',
    'chai',
    'choc',
    'chocolate',
    'cinnamon',
    'clementine',
    'coconut',
    'cran',
    'cream',
    'dessert',
    'drgnfrt',
    'espresso',
    'fruit',
    'ginger',
    'guava',
    'habanero',
    'hazelnut',
    'honey',
    'huckleberry',
    'horseradish',
    'liqueur',
    'melon',
    'mint',
    'mocha',
    'passionfruit',
    'pecan',
    'peppermint',
    'razberi',
    'rosemary',
    'sausage',
    'sriracha',
    'sugar',
    'tamarind',
    'basil', 
    'rose', 
    'aprct', 
    'dill',
    'elderflower',
    'rdbry',
    'tropical',
    'punch',
    'passion',
    'crml',
]

flavor_keywords = [
    'flavored',
    'infused',
    'peach',
    'raspberry',
    'cherry',
    'orange',
    'pineapple',
    'strawberry',
    'lme',
    'infusions',
    'mango',
    'bl',
    'berry',
    'citron',
    'vanilla',
    'watrmln',
    'cucumber',
    'sweat',  # check this one
    'blueberry',
    'wild', # check
    'cranberry',
    'caramel',
    'botanical',
    'mandrin',
    'pepper',
    'juice',
    'spicy',
    'chili',
    'pomegranate',
    'salted',
    'mrionbrry',
    'hckebry',
    'essences',
]

blacklist = {
    'orange': {'dog', 'rng', 'rán'},
    'bl': {'bld', 'dbl'},
    'pepper': {'tears', 'texas', 'pearl'},
    'salted': {'salute'},
}

overrides = {
    'pink': {'pink', 'pnk'},
    'picle': {'pickle', 'pkl'},
    'flv': {'flv'},
    'grape': {'grape', 'grape-based', 'grapefruit', 'grpfrt', 'grpfrt/drgn'},
    'lemonade': {'lemlm', 'lemonade', 'lemongrass', 'limeade', 'limoncello'},
    'lavender': {'lavender', 'lvndr'},
    'tea': {'tea', 'tea-sweet'},
    'whipped': {'whipped', 'whpd'},
}

# COMMAND ----------

# Modify partition list
partitions_mod = {k: partitions[k] for k in flavor_keywords}

for k, v in blacklist.items():
    partitions_mod[k] = [i for i in partitions_mod[k] if i not in v]

for k, v in overrides.items():
    partitions_mod[k] = v

all_keywords = set().union(*partitions_mod.values()) | set(one_offs)
all_keywords

# COMMAND ----------

# is_vodka()
name = 'CIROC Peach (Made with Vodka Infused with Natural Flavors)'
[word.lower().strip('()[]{}™®')  for word in name.split()]

# COMMAND ----------

def create_keyword_matcher(all_keywords, return_list=False):
    all_keywords = set(all_keywords)

    def is_match(name):
        return any(word.lower().strip('()[]{}™®') in all_keywords for word in name.split())
    
    def get_matches(name):
        return [word_mod for word in name.split() if (word_mod := word.lower().strip('()[]{}™®')) in all_keywords]
    
    return get_matches if return_list else is_match

is_vodka = create_keyword_matcher(all_keywords)

VodkaUDF = f.udf(is_vodka, returnType=BooleanType())

# Get the list
flagged = all_vodka.withColumn('flavored', VodkaUDF(f.col('pim_description')))

(
    flagged
    .groupBy('fyt_sub_com_cct_dsc_tx')
    .agg(f.sum(f.col('flavored').cast(IntegerType())).alias('num_flagged'),
         f.count('*').alias('total_num'))
    .withColumn('prop_flagged', f.col('num_flagged') / f.col('total_num'))
    .orderBy('total_num', ascending=False)
    .display()
)

# COMMAND ----------

flagged.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. ChatGPT Approach
# MAGIC
# MAGIC Prompts Used:
# MAGIC
# MAGIC ```txt
# MAGIC Create an exhaustive table of flavored vodka keywords and the categories they belong to. Create this table in CSV format. The first few rows of the table are given below:
# MAGIC
# MAGIC Keyword,Category
# MAGIC raspberry,fruit
# MAGIC strawberry,fruit
# MAGIC peach,fruit
# MAGIC cinnamon,spice
# MAGIC chocolate,dessert
# MAGIC
# MAGIC Continue the list.
# MAGIC ```
# MAGIC
# MAGIC I added `flavored` and `infused` manually.

# COMMAND ----------

chatgpt_str = """flavored,meta
infused,meta
raspberry,fruit
strawberry,fruit
peach,fruit
cinnamon,spice
chocolate,dessert
vanilla,dessert
coconut,tropical
pineapple,tropical
mango,tropical
watermelon,fruit
lime,citrus
lemon,citrus
orange,citrus
grapefruit,citrus
apple,fruit
pear,fruit
blueberry,fruit
blackberry,fruit
cranberry,fruit
pomegranate,fruit
melon,fruit
banana,fruit
kiwi,fruit
peppermint,mint
spearmint,mint
coffee,caffeine
espresso,caffeine
caramel,dessert
butterscotch,dessert
bubblegum,sweet
cotton candy,sweet
marshmallow,dessert
cake,dessert
cookie,dessert
candy,confectionery
melon,tropical
tutti frutti,sweet
hibiscus,floral
lavender,floral
rose,floral
jasmine,floral
ginger,spice
pepper,spice
chili,spice
wasabi,spice
mint,herb
basil,herb
thyme,herb
dill,herb
coriander,herb
cilantro,herb
lemongrass,herb
sage,herb
parsley,herb
anise,spice
clove,spice
nutmeg,spice
cardamom,spice
gingerbread,dessert
toffee,dessert
caramel apple,dessert
mocha,caffeine
candy cane,sweet
gingerbread,spice
cherry,fruit
passionfruit,fruit
guava,fruit
dragonfruit,fruit
lychee,fruit
persimmon,fruit
fig,fruit
papaya,fruit
apricot,fruit
nectarine,fruit
plum,fruit
elderflower,floral
honeysuckle,floral
violet,floral
orchid,floral
iris,floral
lilac,floral
honeysuckle,floral
hibiscus,floral
jasmine,floral
chamomile,floral
lavender,floral
passionflower,floral
rosehip,floral
marigold,floral
bergamot,floral
hibiscus,floral
cherry blossom,floral
peony,floral
lotus,floral
daisy,floral
tulip,floral
sunflower,floral
honeysuckle,floral
orange blossom,floral
mint chocolate,dessert
key lime pie,dessert
cookie dough,dessert
pumpkin spice,dessert
peanut butter,dessert
maple syrup,dessert
marshmallow fluff,dessert
cinnamon roll,dessert
s'mores,dessert
brownie,dessert
red velvet,dessert
cheesecake,dessert
lemon meringue,dessert
caramel macchiato,dessert
candy corn,sweet
rock candy,sweet
caramel popcorn,sweet
gummy bear,sweet
cotton candy,sweet
caramel apple,sweet
candy floss,sweet
bubblegum,sweet
marshmallow,sweet
toasted marshmallow,sweet
cake batter,sweet
root beer,sweet
butterscotch,sweet
honey,sweet
maple,sweet
molasses,sweet
candy cane,sweet
chocolate mint,sweet
milk chocolate,sweet
white chocolate,sweet
dark chocolate,sweet
chocolate orange,sweet
chocolate raspberry,sweet
chocolate-covered strawberry,sweet
chocolate chip,sweet
mint chip,sweet
candy cane,sweet
peppermint bark,sweet
sugar cookie,sweet
gingerbread,sweet
fruitcake,sweet
hot chocolate,sweet
eggnog,sweet
pumpkin pie,sweet
apple pie,sweet
pecan pie,sweet
banana cream pie,sweet
coconut cream pie,sweet
caramel apple pie,sweet
cherry pie,sweet
blueberry pie,sweet
strawberry rhubarb pie,sweet
lemon meringue pie,sweet
key lime pie,sweet
cinnamon bun,sweet
french toast,sweet
caramel macchiato,sweet
irish cream,sweet
amaretto,sweet
kahlua,sweet
baileys,sweet
rumchata,sweet
coquito,sweet
panettone,sweet
fruitcake,sweet
gingerbread,sweet
wassail,sweet
mulled wine,sweet
candy apple,sweet
toffee apple,sweet
caramel apple,sweet
bobbing for apples,sweet
candy corn,sweet
caramel corn,sweet
popcorn ball,sweet
toffee popcorn,sweet
caramelized pear,sweet
caramelized banana,sweet
caramelized fig,sweet
caramelized peach,sweet
caramelized apple,sweet
caramelized pineapple,sweet
caramelized mango,sweet
caramelized coconut,sweet
caramelized papaya,sweet
caramelized guava,sweet
caramelized persimmon,sweet
caramelized apricot,sweet
caramelized nectarine,sweet
caramelized plum,sweet
caramelized cherry,sweet
caramelized strawberry,sweet
caramelized raspberry,sweet
caramelized blueberry,sweet
caramelized blackberry,sweet
caramelized cranberry,sweet
caramelized pomegranate,sweet
caramelized orange,sweet
caramelized grapefruit,sweet
caramelized lemon,sweet
caramelized lime,sweet
caramelized tangerine,sweet
caramelized kiwi,sweet
caramelized passionfruit,sweet
caramelized dragonfruit,sweet
caramelized lychee,sweet
caramelized kiwifruit,sweet
caramelized pineapple,sweet
caramelized papaya,sweet
caramelized guava,sweet
caramelized mango,sweet
caramelized banana,sweet
caramelized peach,sweet
caramelized nectarine,sweet
caramelized apricot,sweet
caramelized plum,sweet
caramelized cherry,sweet
caramelized strawberry,sweet
caramelized raspberry,sweet
caramelized blueberry,sweet
caramelized blackberry,sweet
caramelized cranberry,sweet
caramelized pomegranate,sweet
caramelized orange,sweet
caramelized grapefruit,sweet
caramelized lemon,sweet
caramelized lime,sweet
caramelized tangerine,sweet
caramelized kiwi,sweet
caramelized passionfruit,sweet
caramelized dragonfruit,sweet
caramelized lychee,sweet
caramelized fig,sweet
caramelized kiwifruit,sweet
caramelized persimmon,sweet
caramelized fig,sweet
caramelized kiwifruit,sweet
caramelized persimmon,sweet
caramelized fig,sweet
caramelized kiwifruit,sweet
caramelized persimmon,sweet
caramelized fig,sweet
caramelized kiwifruit,sweet
caramelized persimmon,sweet
caramelized fig,sweet
caramelized kiwifruit,sweet
caramelized persimmon,sweet
caramelized fig,sweet
caramelized kiwifruit,sweet
caramelized persimmon,sweet
caramelized fig,sweet
caramelized kiwifruit,sweet
caramelized persimmon,sweet
caramelized fig,sweet
caramelized kiwifruit,sweet
caramelized persimmon,sweet
caramelized fig,sweet
caramelized kiwifruit,sweet
caramelized persimmon,sweet
caramelized fig,sweet
caramelized kiwifruit,sweet
caramelized persimmon,sweet
caramelized fig,sweet
caramelized kiwifruit,sweet
caramelized persimmon,sweet
"""

chatgpt_table = spark.createDataFrame(
    [row.split(',') for row in chatgpt_str.split('\n') if len(row) > 0],
    ['Keyword', 'Category']
).distinct()
chatgpt_table.display()

# COMMAND ----------

chatgpt_table.groupBy('Category').count().orderBy('count', ascending=False).display()

# COMMAND ----------

chatgpt_categories = [row.Keyword for row in chatgpt_table.collect()]
print(f'{len(chatgpt_categories)} categories')

# COMMAND ----------

def build_up_groups(categories, items, thresh=80):
    # Initialize output
    out = {}

    # Initialize mapping
    item_index_mapping = {v:i for i,v in enumerate(items)}

    # Compute similarities
    categories_to_items = np.array(
        [[match_string(category, item)
          for item in items]
         for category in categories]
    )
    items_to_items = np.array(
        [[match_string(item1, item2)
          for item2 in items]
         for item1 in items]
    )

    # Get similar category/item pairs
    for cat_idx, item_idx in zip(*np.asarray(categories_to_items>thresh).nonzero()):
        print(categories[cat_idx], items[item_idx])
        out.setdefault(categories[cat_idx], []).append(items[item_idx])

    def expand_frontier(sub_items):
        item_idxs = [item_index_mapping[i] for i in sub_items]
        subset = items_to_items[item_idxs]
        _, all_qualifying = np.asarray(subset>thresh).nonzero()
        new_items = [items[i] for i in all_qualifying
                     if i not in item_idxs]
        return sub_items + new_items

    # Continue to fill in
    for key in out.keys():
        curr_len = len(out[key])
        done = False

        while not done:
            out[key] = expand_frontier(out[key])
            if len(out[key]) == curr_len:
                done = True
            curr_len = len(out[key])

    return out


keyword_expansion = build_up_groups(chatgpt_categories, [i[0] for i in word_counts])
keyword_expansion

# COMMAND ----------

all_chatgpt_keywords = set().union(*[set(v) for v in keyword_expansion.values()])
all_chatgpt_keywords

# COMMAND ----------

is_vodka_chat = create_keyword_matcher(all_chatgpt_keywords)
VodkaChatUDF = f.udf(is_vodka_chat, returnType=BooleanType())

# Get the list
flagged_chat = all_vodka.withColumn('flavored', VodkaChatUDF(f.col('pim_description')))

(
    flagged_chat
    .groupBy('fyt_sub_com_cct_dsc_tx')
    .pivot('flavored')
    .count()
    .display()
)

# COMMAND ----------

(
    flagged
    .groupBy('fyt_sub_com_cct_dsc_tx')
    .pivot('flavored')
    .count()
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Comparison

# COMMAND ----------

# Keyword comparison
print(f'Common Keywords: {set(all_keywords) & set(all_chatgpt_keywords)}')
print(f'Manual Only: {set(all_keywords) - set(all_chatgpt_keywords)}')
print(f'ChatGPT Only: {set(all_chatgpt_keywords) - set(all_keywords)}')

# COMMAND ----------

# Proportions identified by subcommodity
(
    flagged
    .join(
        flagged_chat
        .select('con_upc_no', 
                f.col('flavored')
                .alias('flavored_chat')),
        on='con_upc_no',
        how='inner'
    )
    .groupBy('fyt_sub_com_cct_dsc_tx')
    .agg(
        f.sum(f.col('flavored').cast(IntegerType())).alias('num_flavored_manual'),
        f.sum(f.col('flavored_chat').cast(IntegerType())).alias('num_flavored_chat'),
        f.count('*').alias('total_num')
    )
    .display()
)

# COMMAND ----------

# Combine all flags
all_flags = (
    flagged
    .join(
        flagged_chat
        .select('con_upc_no', 
                f.col('flavored')
                .alias('flavored_chat')),
        on='con_upc_no',
        how='inner'
    )
    .withColumn('flavored_subcom', f.col('fyt_sub_com_cct_dsc_tx').contains('FLAVORED'))
    .select(
        'gtin_no',
        f.col('flavored').alias('manual_flag'),
        f.col('flavored_chat').alias('chat_flag'),
        f.col('flavored_subcom').alias('subcom_flag'),
        (f.col('flavored') | f.col('flavored_subcom')).alias('manual_aug_flag'),
        (f.col('flavored_chat') | f.col('flavored_subcom')).alias('chat_aug_flag'),
    )
    .cache()
)
all_flags.count()

flag_cols = [col for col in all_flags.columns if 'flag' in col]

# Get ACDS HH lists for each
yearly_transactions = (
    acds.get_transactions(
        start_date='20230101',
        end_date='20231231',
        apply_golden_rules=golden_rules(),
    )
    .join(all_flags, on='gtin_no', how='inner')
    .select('ehhn', *flag_cols)
    .distinct()
    .cache()
)
yearly_transactions.count()

for flag_col in flag_cols:
    print(f'{flag_col}: {yearly_transactions.filter(flag_col).count()}')

# COMMAND ----------

flagged.limit(20).display()

# COMMAND ----------

'gtin_no' in flagged.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Final Output

# COMMAND ----------

all_keywords

# COMMAND ----------

get_keyword_matches = create_keyword_matcher(all_keywords, True)

VodkaMatchUDF = f.udf(get_keyword_matches, returnType=ArrayType(StringType()))

final_output = (
    all_vodka
    .withColumn('keyword_flag', VodkaUDF(f.col('pim_description')))
    .withColumn('sub_com_flag', f.col('fyt_sub_com_cct_dsc_tx').contains('FLAVORED'))
    .withColumn('overall_flag', f.col('keyword_flag') | f.col('sub_com_flag'))
    .withColumn('matching_keywords', VodkaMatchUDF(f.col('pim_description')))
    .select('con_upc_no', *fields, 'keyword_flag', 'sub_com_flag', 'overall_flag', 'matching_keywords')
)

final_output.display()

# COMMAND ----------

out_path = 'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/flavored_vodka_poc'
final_output.write.mode('overwrite').parquet(out_path)
