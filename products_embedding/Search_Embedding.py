# Databricks notebook source
# Import service principal definitions functions, date calculation and hard coded configs from config
import resources.config as config
import src.utils as utils
import products_embedding.src.date_calcs as dates
from products_embedding.config import sentences_dict

#Set-up widgets
config.dbutils.widgets.text("embedding_name", "")
config.dbutils.widgets.text("embedding_sentence_query", "")

# COMMAND ----------

#Read in latest product vectors that will be used in dot product
product_vectors_path = config.get_latest_modified_directory(config.product_vectors_description_path)
product_vectors_df = config.spark.read.format("delta").load(product_vectors_path)

#Get boolean that checks if the notebook is running via widgets
embedding_name_check = "embedding_name"
embedding_sentence_query_check = "embedding_sentence_query"
is_empty = utils.pyspark_databricks_widget_check_for_empty_text_field(embedding_name_check)

#If the notebook is operating via widget...
if not is_empty:
  #Extract the inputted parameters
  name = config.dbutils.widgets.get("embedding_name")
  sentence_query = config.dbutils.widgets.get("embedding_sentence_query")
  sentence_query = ''.join(sentence_query).lower()
  #Put parameters in dictonary format so that for-loop below
  #operates as expected.
  sentences_dict = {name: sentence_query}
#Just pull the control dict that has the search sentence for each audience
else:
  sentences_dict = sentences_dict

for name in sentences_dict.keys():
  #Pull relevant sentence query
  sentence_query = sentences_dict[name]
  #Assert sentence query is a string
  #Encoding the query and convert into list format to do the dot product
  query_vector = utils.model.encode(sentence_query, normalize_embeddings=True).tolist()
  #Do dot product between search query and vector from product embedding
  search_df = utils.create_search_df(utils.create_dot_df(product_vectors_df,
                                                          utils.create_array_query(query_vector)))
  #Clean up audience name for more manageable format when writing out the directory
  name = name.replace(' ', '_')
  name = name.replace('/', '')
  #Output search dataframe that outputted from the dot product for given audience
  output_fp = config.diet_query_embeddings_dir + dates.today + '/' +name
  search_df.\
  write.\
  mode("overwrite").\
  format("delta").\
  save(output_fp)

  message = (
    "Outputted {} for the given audience and query below \n".format(output_fp) +
    "{}: {} \n \n".format(name, sentence_query)
  )
  print(message)

#Code ran successfully, but warn the user that they
#need to add inputted parameters to sentence_dict
if not is_empty:
  message = (
    'UPC list was successfully created for {}! '.format(name) +
    'Make sure to add the below key-value pair to the sentence_dict and git push it if you would like to it included in the automated workflow.' +
    '\n' + '\n' +
    '"{}": "{}"'.format(name, sentence_query)
  )
  raise Warning(message)

# COMMAND ----------

config.dbutils.notebook.exit("Search_Embedding completed")
