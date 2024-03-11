from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    MapType, 
    DoubleType,
    IntegerType
)

# Helper functions
def spark_to_file(path: str) -> str:
    if not path.startswith('dbfs:/'):
        raise ValueError(f'Path {path} is not in Spark API format.')
    return f'/dbfs/{path[6:]}'

def file_to_spark(path: str) -> str:
    if not path.startswith('/dbfs/'):
        raise ValueError(f'Path {path} is not in file API format.')
    return f'dbfs:/{path[6:]}'

# Paths
base_path = f'dbfs:/FileStore/Users/p870220/non_endemic_cluster_testing_ngr'

embedding_path = f'{spark_to_file(base_path)}/embeddings.npy'
upc_path = f'{spark_to_file(base_path)}/upc_list.pkl'
bias_path = f'{spark_to_file(base_path)}/biases.pkl'

log_paths = {
    'dimensionality': f'{base_path}/dimensionality_reduction_log',
    'clustering': f'{base_path}/clustering_log',
    'stats': f'{base_path}/cluster_stat_log',
}

artifact_paths = {
    'dimensionality': f'{base_path}/dimensionality_reduction_artifacts',
    'clustering': f'{base_path}/clustering_artifacts',
}

notebook_names = {
    'main': 'hyperparameter_sweep',
    'pca': 'dr_pca',
    'tSNE': 'dr_tSNE',
    'UMAP': 'dr_UMAP',
    'kmeans': 'c_kmeans',
    'HDBSCAN': 'c_hdbscan',
    'OPTICS': 'c_optics',
    'stats': 'cluster_evaluation',
}

# Log schema
dr_log_schema = StructType([
    StructField("unique_id", StringType(), True),
    StructField("algorithm_type", StringType(), True),
    StructField("hparam_dict", StringType(), True),
    StructField("trustworthiness", DoubleType(), True)
])

cluster_log_schema = StructType([
    StructField("unique_id", StringType(), True),
    StructField("dr_id", StringType(), True),
    StructField("algorithm_type", StringType(), True),
    StructField("hparam_dict", StringType(), True),
    StructField("num_clusters", IntegerType(), True)
])

stats_log_schema = StructType([
    StructField("unique_id", StringType(), True),
    StructField("dr_id", StringType(), True),
    StructField("min_cluster_size", IntegerType(), True),
    StructField("q1_cluster_size", IntegerType(), True),
    StructField("median_cluster_size", IntegerType(), True),
    StructField("q3_cluster_size", IntegerType(), True),
    StructField("max_cluster_size", IntegerType(), True),
    StructField("mean_cluster_size", DoubleType(), True),
    StructField("std_cluster_size", DoubleType(), True),
    StructField("base_silhouette_score", DoubleType(), True),
    StructField("dr_silhouette_score", DoubleType(), True),
    StructField("base_ch_score", DoubleType(), True),
    StructField("dr_ch_score", DoubleType(), True),
    StructField("base_db_score", DoubleType(), True),
    StructField("dr_db_score", DoubleType(), True),
])

# Hyperparameters
dimension_params = [2, 10, 50, 100]
distance_metrics = ['euclidean', 'cityblock', 'cosine']

dimensionality_reduction_hyperparameters = {
    'identity': {},
    'pca': {'n_components': dimension_params},
    'tSNE': {
        'n_components': dimension_params,
        'metric': distance_metrics,
        'perplexity': [5, 30, 45],
    },
    'UMAP': {
        'n_components': dimension_params,
        'n_neighbors': [5, 30, 100],
        'min_dist': [0, .1, .5],
    }
}

cluster_sizes = [5, 20, 50]

clustering_hyperparameters = {
    'kmeans': {},
    'HDBSCAN': {
        'min_cluster_size': cluster_sizes,
        'cluster_selection_epsilon': [0, 0.5],
    },
    'OPTICS': {'min_samples': cluster_sizes},
}

# Constants
EMBEDDING_DATA_WEEK = '20231210'
UNIVERSE_SIZE = 1000
MAX_EXECUTION_TIME = 60*60*4
MAX_SILHOUETTE_SIZE = 100_000