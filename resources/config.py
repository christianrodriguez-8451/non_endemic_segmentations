# spark
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# Define service principals
service_credential = dbutils.secrets.get(scope='kv-8451-tm-media-dev', key='spTmMediaDev-pw')
service_application_id = dbutils.secrets.get(scope='kv-8451-tm-media-dev', key='spTmMediaDev-app-id')
directory_id = "5f9dc6bd-f38a-454a-864c-c803691193c5"
storage_account = ['sa8451posprd', 'sa8451dbxadhocprd', 'sa8451entlakegrnprd']

# Set configurations
for sa in storage_account:
    spark.conf.set(f"fs.azure.account.auth.type.{sa}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{sa}.dfs.core.windows.net",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{sa}.dfs.core.windows.net", service_application_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{sa}.dfs.core.windows.net", service_credential)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{sa}.dfs.core.windows.net",
                   f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# Define root directories and sub directories.
embedded_dimensions_dir = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions'
vintages_dir = '/customer_data_assets/vintages'
segment_behavior_dir = '/customer_data_assets/segment_behavior'
azure_pinto_path = 'abfss://data@sa8451entlakegrnprd.dfs.core.windows.net/source/third_party/prd/pinto/'
azure_pim_core_by_cycle = "abfss://pim@sa8451posprd.dfs.core.windows.net/pim_core/by_cycle/"
product_vectors_dir = '/product_vectors_diet_description/cycle_date='
product_vectors_description_path = \
    "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/embedded_dimensions/product_vectors_diet_description/"
diet_query_embeddings_dir = embedded_dimensions_dir + '/diet_query_embeddings/cycle_date='
diet_query_dir = '/diet_query_embeddings'
diet_upcs_dir = embedded_dimensions_dir + '/diet_upcs/cycle_date='
egress_dir = embedded_dimensions_dir + '/egress/upc_list'


def get_latest_modified_directory(pDirectory):
    """
    get_latest_modified_file_from_directory:
      For a given path to a directory in the data lake, return the directory that was last modified.
      Input path format expectation: 'abfss://x@sa8451x.dfs.core.windows.net/
    """
    # Set to get a list of all folders in this directory and the last modified date time of each
    vDirectoryContentsList = list(dbutils.fs.ls(pDirectory))

    # Convert the list returned from get_dir_content into a dataframe so we can manipulate the data easily. Provide it
    # with column headings.
    # You can alternatively sort the list by LastModifiedDateTime and get the top record as well.
    df = spark.createDataFrame(vDirectoryContentsList, ['FullFilePath', 'LastModifiedDateTime'])

    # Get the latest modified date time scalar value
    maxLatestModifiedDateTime = df.agg({"LastModifiedDateTime": "max"}).collect()[0][0]

    # Filter the data frame to the record with the latest modified date time value retrieved
    df_filtered = df.filter(df.LastModifiedDateTime == maxLatestModifiedDateTime)

    # return the file name that was last modifed in the given directory
    return df_filtered.first()['FullFilePath']
