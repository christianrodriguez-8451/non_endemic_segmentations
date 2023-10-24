# Databricks notebook source
# Import service principal definitions functions, date calculation and hard coded configs from config
import resources.config as config
import src.utils as utils
import products_embedding.src.date_calcs as dates
# COMMAND ----------

# Set path for product vectors
product_vectors_path = config.get_latest_modified_directory(config.product_vectors_description_path)
product_vectors_df = config.spark.read.format("delta").load(product_vectors_path)

# Set path for product vectors
upc_list_path = config.get_latest_modified_directory(config.embedded_dimensions_dir + config.diet_query_dir)
diet_query_embeddings_directories = config.spark.createDataFrame(list(config.dbutils.fs.ls(upc_list_path)))
diet_query_embeddings_directories = \
    diet_query_embeddings_directories.filter(diet_query_embeddings_directories.name !=
                                             'embedding_sentence_query_lookup/')
diet_query_embeddings_directories_list = diet_query_embeddings_directories.rdd.map(lambda column: column.name).collect()
embedding_sentence_query_lookup = upc_list_path + 'embedding_sentence_query_lookup'
embedding_sentence_query_lookup_df = config.spark.read.format("delta").load(embedding_sentence_query_lookup)

config.dbutils.widgets.text("embedding_name", "")
config.dbutils.widgets.text("embedding_sentence_query", "")
embedding_name_check = "embedding_name"
embedding_sentence_query_check = "embedding_sentence_query"
is_empty = utils.pyspark_databricks_widget_check_for_empty_text_field(embedding_name_check)
embedding_name = config.dbutils.widgets.get("embedding_name")
embedding_sentence_query = config.dbutils.widgets.get("embedding_sentence_query")


# COMMAND ----------

# taking dot product of query vector and product vector
# order by dot product descending

if not is_empty:
    embedding_sentence_query = ''.join(embedding_sentence_query).lower()
    # Encoding the query, make it a pyspark vector we can take the dot product of later
    query_vector = utils.model.encode(embedding_sentence_query, normalize_embeddings=True).tolist()
    search_df = utils.create_search_df(utils.create_dot_df(product_vectors_df,
                                                           utils.create_array_query(query_vector)))
    embedding_name = embedding_name.replace(' ', '_')
    embedding_name = embedding_name.replace('/', '')
    search_df.write.mode("overwrite").format("delta").save(upc_list_path + embedding_name)
    print(f'Wrote {embedding_name} upc list here: {upc_list_path}')
    data = [(embedding_name, embedding_sentence_query)]
    columns = ["name", "query"]
    embedding_sentence_query = config.spark.createDataFrame(data, columns)
    embedding_sentence_query_lookup_df = embedding_sentence_query_lookup_df.union(embedding_sentence_query)
    # write look up file of upc list location
    embedding_sentence_query_lookup_df.write.mode("overwrite").format("delta").save(upc_list_path
                                                                                    +
                                                                                    '/embedding_sentence_query_lookup')
    print(f'Wrote {embedding_sentence_query} query here: {upc_list_path}')
else:
    embedding_names = diet_query_embeddings_directories_list
    embedding_sentence_query_lookup_df.write.mode("overwrite").format("delta").save(config.diet_query_embeddings_dir
                                                                                    + dates.today +
                                                                                    '/embedding_sentence_query_lookup')
    for names in embedding_names:
        embedding_sentence_query = \
            embedding_sentence_query_lookup_df.select(embedding_sentence_query_lookup_df.query
                                                      ).where(embedding_sentence_query_lookup_df.name == names)
        embedding_sentence_query = embedding_sentence_query.rdd.map(lambda column: column.query).collect()
        embedding_sentence_query = " ".join(embedding_sentence_query)
        # Encoding the query, make it a pyspark vector we can take the dot product of later
        query_vector = utils.model.encode(embedding_sentence_query, normalize_embeddings=True).tolist()
        search_df = utils.create_search_df(utils.create_dot_df(product_vectors_df,
                                                               utils.create_array_query(query_vector)))
        names = names.replace(' ', '_')
        names = names.replace('/', '')
        search_df.write.mode("overwrite").format("delta").save(config.diet_query_embeddings_dir + dates.today + '/' +
                                                               names)


# COMMAND ----------

config.dbutils.notebook.exit("Search_Embedding completed")
