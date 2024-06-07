# Databricks notebook source
# MAGIC %md
# MAGIC # NGR Testing
# MAGIC
# MAGIC The goal of this notebook is to test the [NGR methodology](https://confluence.kroger.com/confluence/display/8451SE/Next+Generation+Tensor+Factorization+Machine). Along with exploring the model and its embedding matrices, we explore the creation of segments using complementary goods and the product hierarchy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %pip install effo_embeddings fuzzywuzzy

# COMMAND ----------

# Python
import numpy as np
from fuzzywuzzy import fuzz

# Spark
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType

# Enterprise
from effo_embeddings.core.nextgenrec import NextGenRec 
from effodata import ACDS, golden_rules

# COMMAND ----------

acds = ACDS(use_sample_mart=False)
rec = NextGenRec(model_as_of="current_ngr", embd_dim_subset = 100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Exploration

# COMMAND ----------

sample_upc_list = ["0000000004011", "0001111050659"]

# COMMAND ----------

# Get embeddings by UPC
rec.get_product_embeddings(sample_upc_list)

# COMMAND ----------

ehhn = [d.ehhn for d in rec.ehhn_df.select("ehhn").collect()]
rec.get_top_k_for_product(sample_upc_list, ehhn=ehhn)

# COMMAND ----------

# MAGIC %debug

# COMMAND ----------

ehhn = [d.ehhn for d in rec.ehhn_df.select("ehhn").collect()]
out = rec.get_top_k(prod=sample_upc_list, ehhn=ehhn)

# COMMAND ----------

out = _

# COMMAND ----------

out = list(out)
out

# COMMAND ----------

next(out)

# COMMAND ----------

prod = rec.get_product_embeddings()
prod

# COMMAND ----------

upc_list = prod['domupc']
len(upc_list)

# COMMAND ----------



# COMMAND ----------

# Get ACDS product list
prods = acds.products
prods.limit(20).display()

# COMMAND ----------

txns = acds.get_transactions(
    start_date='20230101',
    end_date='20231231',
    apply_golden_rules=golden_rules()
)
txns

# COMMAND ----------

upcs = [i[0] for i in txns.select('gtin_no').distinct().collect()]
len(upcs)

# COMMAND ----------

help(acds.get_transactions)

# COMMAND ----------



# COMMAND ----------

prods.count()

# COMMAND ----------



# COMMAND ----------

e = Embedding('nextgenrecommender_1.0')
e

# COMMAND ----------

rec.ehhn_df.limit(20).display()

# COMMAND ----------

help(rec.get_top_k)

# COMMAND ----------

dir(rec)

# COMMAND ----------

Embedding.get_embedding_info('')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sample Sub-Commodity
# MAGIC
# MAGIC The full list of preliminary commodities and sub-commodities we have been tasked with exploring is:
# MAGIC
# MAGIC * HISPANIC BEER (this is a sub-commodity)
# MAGIC * GRILLS (this is a commodity)
# MAGIC * CHARCOAL (this is a commodity)
# MAGIC * CHRISTMAS (this is a commodity)
# MAGIC * HALLOWEEN (this is a commodity)
# MAGIC * EASTER (this is a commodity)
# MAGIC * SCHOOL (this is a commodity)
# MAGIC * ADULT INCONTINENCE (this is a commodity)
# MAGIC * DIAPERS & DISPOSABLES (this is a commodity)
# MAGIC
# MAGIC We will start with "HISPANIC BEER" since it is potentially smaller in scope.

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Construct UPC List

# COMMAND ----------

# Apply Fuzzy Function
def matchstring(s1, s2):
    return fuzz.token_sort_ratio(s1, s2)

MatchUDF = f.udf(matchstring, StringType())

# COMMAND ----------

# Figure out proper sub-commodity name
(
    acds
    .products
    .select('fyt_sub_com_cct_dsc_tx')
    .distinct()
    .withColumn('match_strength', 
                MatchUDF(f.lit('HISPANIC BEER'), 
                         f.col('fyt_sub_com_cct_dsc_tx'))
                .cast(IntegerType()))
    .orderBy(f.col('match_strength'), ascending=False)
    .limit(5)
    .display()
)

# COMMAND ----------

filter_cond = f.col('fyt_sub_com_cct_dsc_tx') == '27101 HISPANIC BEER'

all_products = acds.products.filter(filter_cond)
upc_list = [row.con_upc_no for row in all_products.collect()]
print(f'Found {len(upc_list)} UPCs')
all_products.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Pull Similar HHs

# COMMAND ----------

# Parameters
sample_size = 1_000_000
memoize_db = False
prod = upc_list

# Load inputs
hh_universe = [d.ehhn for d in rec.ehhn_df.select("ehhn").filter(f.col('ehhn').isNotNull()).collect()]
curr_sample = hh_universe[:sample_size]

# Pull product embeddings
prod_map = rec.get_product_embeddings(prod, memoize_db=memoize_db)
prod_map.keys()

# COMMAND ----------

# Get predictions
preds = rec.get_scores(curr_sample, prod_map, memoize_db=memoize_db)
preds.shape

# COMMAND ----------

# Find top k
sort_idxs = np.argsort(preds, axis=0)[::-1]
sorted_preds = np.take_along_axis(preds, sort_idxs, axis=0)
sorted_preds.shape

# COMMAND ----------

# MAGIC %md
# MAGIC A few options from here:
# MAGIC - average/sum over products
# MAGIC - do each separately and compare the results

# COMMAND ----------

# MAGIC %md
# MAGIC #### i. Each Separately

# COMMAND ----------

k = 500

relevant_idxs = sort_idxs[:k]
relevant_idxs.shape

# COMMAND ----------

all_hh_idxs = relevant_idxs.ravel()
all_hh_idxs.shape

# COMMAND ----------

all_hhs = list(set(np.array(curr_sample)[all_hh_idxs]))
len(all_hhs)

# COMMAND ----------

def get_embedding_subset(prod_map, start, end):
    return {k: v[start:end] for k,v in prod_map.items()}

# COMMAND ----------

prod_map_full = rec.get_product_embeddings('*', memoize_db=memoize_db)
# emb_subset = get_embedding_subset(prod_map_full, 0, 1000)

# preds_cust = rec.get_scores(all_hhs, emb_subset, memoize_db=memoize_db)
# preds_cust

# Feed back in to find projects
# preds_cust = rec.get_scores(all_hhs, prod_map_full, memoize_db=memoize_db)
# preds_cust.shape
block_size = 100
it = 0
all_preds = []
while it < len(all_hhs):
    ret_ehhn = all_hhs[it : it + block_size]
    preds = rec.get_scores(ret_ehhn, prod_map_full, memoize_db=memoize_db)
    sort_idxs = preds.argsort(axis=1)[:, ::-1]
    # preds = np.take_along_axis(preds, sort_idxs, axis=1)[:, :k]
    all_preds.append((preds, sort_idxs))
#     ret_prods = np.take_along_axis(
#         np.array(prod).reshape(1, -1), sort_idxs, axis=1
#     )[:, :k]
#     if it < len(ehhn):
#         yield {"ehhn": ret_ehhn, "domupc": ret_prods, "score": preds}
    it += block_size

# return {"ehhn": ret_ehhn, "domupc": ret_prods, "score": preds}

# COMMAND ----------

all_preds[1][1].shape

# COMMAND ----------

preds_cust.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploration

# COMMAND ----------

sorted_preds[:, 0]

# COMMAND ----------

# Get small subset for each UPC
ehhn = [d.ehhn for d in rec.ehhn_df.select("ehhn").filter(f.col('ehhn').isNotNull()).collect()]
# top_k = rec.get_top_k_for_product(prod=upc_list, ehhn=ehhn, k=10)
# top_k
len(ehhn)

# COMMAND ----------

import numpy as np

memoize_db = False
block_size = 1000000#len(ehhn)
prod = upc_list
k = 100

# prod_map = rec.get_product_embeddings(prod, memoize_db=memoize_db)

# # score the first block of customers
# ret_ehhn = ehhn[:block_size]
# preds = rec.get_scores(ret_ehhn, prod_map, memoize_db=memoize_db)

# sort_idxs = np.argsort(preds, axis=0)[::-1]
# preds = np.take_along_axis(preds, sort_idxs, axis=0)[:k, :]
# ret_ehhn = np.take_along_axis(
#     np.repeat(ret_ehhn, len(prod)).reshape(len(ret_ehhn), -1), sort_idxs, axis=0
# )[:k, :]

tmp =  np.repeat(ret_ehhn, len(prod)).reshape(len(ret_ehhn), -1)
# tmp2 = np.take_along_axis(tmp, sort_idxs, axis=0)

print(preds.shape, tmp.shape, sort_idxs.shape)

# # initialize the upc hotlist array (could be a priority queue)
# upc_top_lists = [[]] * len(prod)
# for it_p in range(len(prod)):
#     upc_top_lists[it_p] = [
#         preds[:, it_p].T.tolist(),
#         ret_ehhn[:, it_p].T.tolist(),
#     ]

# for it in range(block_size, len(ehhn), block_size):
#     ret_ehhn = ehhn[it : it + block_size]
#     preds = self.get_scores(ret_ehhn, prod_map, memoize_db=memoize_db)

#     sort_idxs = np.argsort(preds, axis=0)[::-1]
#     preds = np.take_along_axis(preds, sort_idxs, axis=0)[:k, :]
#     ret_ehhn = np.take_along_axis(
#         np.repeat(ret_ehhn, len(prod)).reshape(len(ret_ehhn), -1),
#         sort_idxs,
#         axis=0,
#     )[:k, :]
#     # np.take_along_axis( np.array(ret_ehhn) , sort_idxs, axis = 0)[:k,:]

#     # update lists with new best ehhns per upc
#     for it_p in range(len(prod)):
#         upc_top_lists[it_p][0].extend(preds[:, it_p].T.tolist())
#         upc_top_lists[it_p][1].extend(ret_ehhn[:, it_p].T.tolist())
#         sort_idxs = (np.argsort(upc_top_lists[it_p][0])[::-1])[:k]
#         upc_top_lists[it_p][0] = np.array(upc_top_lists[it_p][0])[
#             sort_idxs
#         ].tolist()
#         upc_top_lists[it_p][1] = np.array(upc_top_lists[it_p][1])[
#             sort_idxs
#         ].tolist()

# return {
#     "domupc": prod,
#     "ehhn": [upc_top_lists[i][1] for i in range(len(prod))],
#     "score": [upc_top_lists[i][0] for i in range(len(prod))],
# }

# COMMAND ----------

prod_map['embedding'].shape

# COMMAND ----------

prod_map['domupc']

# COMMAND ----------

preds[:,5]

# COMMAND ----------

type(ehhn[0])

# COMMAND ----------

[i for i in ehhn if not isinstance(i, int)]

# COMMAND ----------

set(prod_map['domupc']) & set([int(i) for i in upc_list])

# COMMAND ----------

help(rec.get_top_k)

# COMMAND ----------

n = next(top_k)
n

# COMMAND ----------

n.keys()

# COMMAND ----------

n['domupc'].shape

# COMMAND ----------

len(n['ehhn'])

# COMMAND ----------

n['score'].shape

# COMMAND ----------


