# Databricks notebook source
# MAGIC %md
# MAGIC # Keyword Analysis
# MAGIC
# MAGIC This notebook can be used to efficiently design a segment based on keyword matching.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialization

# COMMAND ----------

# MAGIC %pip install fuzzywuzzy

# COMMAND ----------

# Python imports
import re

# PySpark imports
import pyspark.sql.functions as f

# 84.51 imports
from effodata import ACDS, golden_rules

# Local imports
import config, utils

# COMMAND ----------

# Initialize ACDS
acds = ACDS(use_sample_mart=False)

# COMMAND ----------

# Get this week's PIM
pim = utils.get_most_recent_file(config.pim_base_path)

# Create product DataFrame
products = (
    acds.products
    .join(
        pim.select(
            f.col('upc_key').alias('con_upc_no'),
            f.col('krogerOwnedEcommerceDescription').alias('pim_description'),
        ).distinct(),
        on='con_upc_no',
        how='left'
    )
    .select(*config.product_cols)
)

products.limit(20).display()

# COMMAND ----------

products.count(), products.select('con_upc_no').distinct().count()

# COMMAND ----------

acds.products.limit(20).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Narrowing Down
# MAGIC
# MAGIC The first step is to restrict ourselves to a desired subset of the product tree. The fewer UPCs we have to deal with, the easier it will be for us to isolate our keyword list.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option a: Keyword Search
# MAGIC
# MAGIC If you don't know exactly where you want to start, you can start by getting a breakdown of all of the times a given key word or phrase pops up.

# COMMAND ----------

# Get all UPCs containing any of a list of key phrases anywhere in their hierarchy
search_keyphrases = ['vodka']

keyword_matches = products.filter(utils.multiway_keyphrase_matching(config.product_cols, search_keyphrases))

print(f'Matching returned {keyword_matches.count()} products.')
keyword_matches.limit(20).display()

# COMMAND ----------

# Get counts at each level of the product hieararchy
utils.display_product_hierarchy(keyword_matches)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option b: Known Hierarchy
# MAGIC
# MAGIC If you know which parts of the hierarchy you want to filter to, populate the `hierarchy_matches` variable with your choice. It will filter down to just those values.

# COMMAND ----------

# Set the following variable
hierarchy_matches = {
    'fyt_pmy_dpt_cct_dsc_tx': [],
    'fyt_rec_dpt_cct_dsc_tx': [],
    'fyt_sub_dpt_cct_dsc_tx': [],
    'fyt_com_cct_dsc_tx': ['260 VODKA'],
    'fyt_sub_com_cct_dsc_tx': [],
}

hierarchy_prods = products.filter(utils.multiway_exact_matching(hierarchy_matches))
hierarchy_prods.limit(20).display()

# COMMAND ----------

utils.display_product_hierarchy(hierarchy_prods)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Final DataFrame
# MAGIC
# MAGIC In the cell below, write whatever logic is needed to create a DataFrame named `product_subset` containing the products we want.

# COMMAND ----------

product_subset = hierarchy_prods
product_subset.limit(20).display()

# COMMAND ----------

product_subset.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Keyword Aggregation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Keyword Extraction
# MAGIC
# MAGIC Choose the fields you want to extract keywords from, as well as any characters to strip out.

# COMMAND ----------

keyword_fields = ['pim_description']
mfr_field = 'prn_mfg_dsc_tx'
strip_chars = '()[]{}™®'
drop_manufacturers = True

# COMMAND ----------

regex_blocks = [r'^[0-9.]+ ?(pr?f|pk|pack|oz|ct|lt|mls?|%)?$']

filters = [lambda s: len(s) > 1,
           lambda s: not s.isnumeric(),
           *[lambda s: re.match(p, s) is None for p in regex_blocks]]

keywords = utils.extract_keywords(
    product_subset, 
    keyword_fields, 
    strip_chars,
    True, 
    mfr_field, 
    filters
)
sorted(keywords.items(), key=lambda i: -i[1])

# COMMAND ----------

for k in keywords.keys(): print(k)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cluster Names
# MAGIC
# MAGIC In this section, we run code to consolidate keywords into clusters of similar words. In this section, you just have to set the `similarity_threshold` to a number between 0 and 100. The higher the number, the more selective the resulting clusters will be. A good default is `80`, but feel free to play around with it. If the number is too low, you will get non-sensical mega-clusters which defeat the purpose of your analysis. If it is too high, you will have to look through more orphaned words. Use the printed stats, and especially the "Longest Partition" stat, to determine what threshold to set.

# COMMAND ----------

from itertools import combinations
from utils import match_string

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

similarity_threshold = 83
condensed, misfits = condense_similar_items(keywords.keys(), thresh=similarity_threshold)
get_condensed_stats(condensed, misfits)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Manual Separation
# MAGIC
# MAGIC This section is where the bulk of the manual work has to happen.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper Function
# MAGIC
# MAGIC You can use this function to view the list of products corresponding to a given keyword set.

# COMMAND ----------

import pyspark.sql.types as t
from functools import reduce

def tag_keyword_matches(df,#: DataFrame,
                        keywords,#: List[str],
                        fields,#: List[str],
                        strip_chars,#: str,
                        ):# -> DataFrame:
    all_keywords = set(keywords)
    preprocess_word = lambda word: word.lower().strip(strip_chars)

    @f.udf(returnType=t.BooleanType())
    def is_match(name):
        if name is None:
            return False

        return any(preprocess_word(word) in all_keywords for word in name.split())
    
    @f.udf(returnType=t.StringType())
    def get_matches(name):
        if name is None:
            return []

        return [word_mod for word in name.split() if (word_mod := preprocess_word(word)) in all_keywords]
    
    df = (
        df
        .select(
            *df.columns,
            *[is_match(field).alias(f'{field}_match') for field in fields],
            *[get_matches(field).alias(f'{field}_keywords') for field in fields],
        )
        .withColumn('overall_keyword_match', reduce(lambda a, b: a | b, [f.col(f'{field}_match') for field in fields]))
    )

    return df

check_list = ['lbgtq']
tag_keyword_matches(product_subset, check_list, keyword_fields, strip_chars).filter(f.col('overall_keyword_match')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step a: Clusters

# COMMAND ----------

condensed

# COMMAND ----------

keep_clusters = [
    'brry',
    'stbry',
    'pear',
    'strawberry',
    'lemnade',
    'flavor',
    'infusion',
    'twist',
    'apple',
    'whipped',
    'citron',
    'olives',
    'cucumber',
    'salty',
    'caramel',
    'lime',
    'raspberry',
    'huckleberry',
    'grape',
    'mandrin',
    'orange',
    'marshmallow',
    'cake',
    'wtermeln',
    'vanilla',
    'cinnabon',
    'pepper',
    'pomegrante',
    'lemn',
    'bloody',
    'cranbry',
    'sour',
    'sweet',
    'coconut',
    'smore',
    'blbry',
    'berri',
    'malts',
    'beer',
    'grpft',
    'watrmln',
    'moscato',
    'pepprmnt',
    'spiced',
    'botanical',
    'spcy',
    'candy',
    'aprct',
    'pumpkin',
    'pine',
    'banana',
    'essences',
    'flvr',
    'hcklbry',
    'marionberry',
    'rootbeer',
]

blacklist = {
    'pear': {'pearl'},
    'lime': {'slim'},
    'sour': {'our', 'four'},
}

overrides = {
    'peach': {'peach', 'peachik', 'peac', 'w-peach'},
    'pickle': {'pickle', 'pckle', 'pckl'},
    'orng': {'orng', 'org'},
}

one_offs = [
    'cherry',
    'salted',
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step b: Misfits

# COMMAND ----------

for i, name in enumerate(sorted(misfits)):
    print(name)
    if i > 0 and i % 10 == 0:
        print()

# COMMAND ----------

misfit_matches = """acai
acai-blueberry
agave
basil
bbery
bblgm
bean
blackberry
blacklicorice
bloodymary
blueberi
blueberrybttry
butter
butterscotch
chai
chile
chili
chilkoot
chipotle
cilantro
coffee
cognac-infused
colada
cookie
cookiedough
corncran
cranapple
cream
cynamom
dessert
elderflower
espre
espresso
esprs
fpnch
frosted
frosting
frt
fruit
ginger
grape-based
grapefruit
hazelnut
honey
honeycrisp
horchata
horseradish
jalapeno
juice
juicy
juniper
kiwi
lavender
leches
lel
lemlm
lemongrass
limeade
limon
limonata
limoncello
liqueur
liquorice
liquorizh
mango
maple
margar
margarita
melon
merlot
mimosa
mocha
mshmlw
natural
nectar
passion
passionfruit
peanut
pecan
pntbutter
rdbry
red
redberry
rosemary
salmon
sangria
sausage
schnapps
sriracha
strasberi
sugar
tamarind
tangerine
tomato
tropical
"""

misfit_keywords = misfit_matches.split('\n')
misfit_keywords

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Keyword List
# MAGIC
# MAGIC The following code prints out the full list of keywords to filter on.

# COMMAND ----------

partitions = {k: condensed[k] for k in keep_clusters}

# Remove blacklist
for k, v in blacklist.items():
    partitions[k] = [i for i in partitions[k] if i not in v]

# Add overrides
for k, v in overrides.items():
    partitions[k] = v

all_keywords = list(set().union(*partitions.values()) | set(one_offs) | set(misfit_keywords))

print(f'{len(all_keywords)} keywords identified')
for keyword in sorted(all_keywords): print(keyword)

# COMMAND ----------

flagged = tag_keyword_matches(product_subset, all_keywords, keyword_fields + ['con_dsc_tx'], strip_chars)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation
# MAGIC
# MAGIC Here, we explore the characteristics of our keywords.

# COMMAND ----------

# View tags for whole subset
flagged.display()

# COMMAND ----------

# Get sub-commodity breakdown
(
    flagged
    .withColumn('keyword_match', f.when(f.col('overall_keyword_match'), 1).otherwise(0))
    .groupBy('fyt_sub_com_cct_dsc_tx')
    .agg(f.sum('keyword_match').alias('num_matches'),
         f.count('*').alias('total_num'))
    .withColumn('prop_match', f.col('num_matches') / f.col('total_num'))
    .orderBy('total_num', ascending=False)
    .display()
)

# COMMAND ----------

(
    flagged
    .filter(f.col('fyt_sub_com_cct_dsc_tx') == '36002 TRADITIONAL FLAVORED VODKA')
    .filter(~f.col('overall_keyword_match'))
    .display()
)

# COMMAND ----------

addendums = [
    'pomegrnte',
    'shrbt',
    'vanla',
    'rspbry',
    'tangarine',
    'razberi',
    'bluerasp',
    'blueberry',
    'cupcake',
    'mrshmllw'
]

# COMMAND ----------

all([])

# COMMAND ----------

'360'.isnumeric()

# COMMAND ----------

keywords

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



# COMMAND ----------



# COMMAND ----------

from functools import reduce

keywords = ['vodka']

fields = [
    'con_dsc_tx', 
    'fyt_pmy_dpt_cct_dsc_tx',
    'fyt_rec_dpt_cct_dsc_tx',
    'fyt_sub_dpt_cct_dsc_tx',
    'fyt_com_cct_dsc_tx',
    'fyt_sub_com_cct_dsc_tx',
    'pim_description',
    # 'pinto_description'
]

mega_cond = reduce(
    lambda a, b: a | b, 
    [f.lower(f.col(col_name)).contains(keyword)
     for col_name in fields
     for keyword in keywords]
)
keyword_filtered = products.filter(mega_cond).select('con_upc_no', *fields)
print(f'{keyword_filtered.count()} products returned')
keyword_filtered.display()

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing/cluster_views/unrestricted_pairwise/audiences/group=24176/F20240202/metrics')

# COMMAND ----------



# COMMAND ----------

products.display()
pim.display()

# COMMAND ----------


