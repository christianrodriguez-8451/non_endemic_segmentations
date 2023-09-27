# Databricks notebook source
# Import service principal definitions functions, date calculation and hard coded configs from config
import resources.config as config
import src.utils as utils
import products_embedding.src.date_calcs as dates
# COMMAND ----------

# Set path for product vectors
product_vectors_path = config.get_latest_modified_directory(config.product_vectors_description_path)
product_vectors_df = utils.spark.read.format("delta").load(product_vectors_path)

# Set path for product vectors
upc_list_path = config.get_latest_modified_directory(config.embedded_dimensions_dir + config.diet_query_dir)
diet_query_embeddings_directories = utils.spark.createDataFrame(list(config.dbutils.fs.ls(upc_list_path)))
diet_query_embeddings_directories_list = diet_query_embeddings_directories.rdd.map(lambda column: column.name).collect()

config.dbutils.widgets.text("embedding_name", "")
embedding_name = config.dbutils.widgets.get("embedding_name")
res = any(len(ele) == 0 for ele in embedding_name)
config.dbutils.widgets.text("embedding_sentence_query", "")
embedding_sentence_query = config.dbutils.widgets.get("embedding_sentence_query")

# COMMAND ----------

# taking dot product of query vector and product vector
# order by dot product descending

if not res:
    embedding_sentence_query = ''.join(embedding_sentence_query).lower()
    # Encoding the query, make it a pyspark vector we can take the dot product of later
    query_vector = utils.model.encode(embedding_sentence_query, normalize_embeddings=True).tolist()
    search_df = utils.create_search_df(utils.create_dot_df(product_vectors_df,
                                                           utils.create_array_query(query_vector)))
    embedding_name = embedding_name.replace(' ', '_')
    embedding_name = embedding_name.replace('/', '')
    search_df.write.mode("overwrite").format("delta").save(upc_list_path + embedding_name)
    data = [{"query": embedding_sentence_query}]
    embedding_sentence_query = utils.spark.createDataFrame(data)
    ##write look up file of upc list location
    embedding_sentence_query.write.mode("overwrite").format("delta").save(upc_list_path +
                                                                          '/embedding_sentence_query_lookup')
else:
    embedding_names = diet_query_embeddings_directories_list
    for names in embedding_names:
        # Encoding the query, make it a pyspark vector we can take the dot product of later
        query_vector = utils.model.encode(names, normalize_embeddings=True).tolist()
        search_df = utils.create_search_df(utils.create_dot_df(product_vectors_df,
                                                               utils.create_array_query(query_vector)))
        names = names.replace(' ', '_')
        names = names.replace('/', '')
        search_df.write.mode("overwrite").format("delta").save(config.diet_query_embeddings_dir + dates.today + '/' +
                                                               names)


# COMMAND ----------

config.dbutils.notebook.exit("Search_Embedding completed")
