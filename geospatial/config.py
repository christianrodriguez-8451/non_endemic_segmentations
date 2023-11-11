# storage blob location https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fb3a5c908-a4dc-4b96-b4d7-f738f28a29d9%2FresourceGroups%2Frg-dbxadhoc-prd%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Fsa8451dbxadhocprd/path/media/etag/%220x8D858B981D1BF26%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None 


# DELTA FILES
#How about below instead?
#geospatial_dir = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/geospatial/"
#metro_micro_nonmetro_dir = geospatial_dir + "/metro_micro_nonmetro/"
storage_micro_metro_nonmetro = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/geospatial/metro_micro_nonmetro/'
storage_state = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/geospatial/state/'
storage_media_market = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/geospatial/media_market/'
storage_census_region = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/geospatial/census_region/'
storage_census_division = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/geospatial/census_division/'

# PARQUET FILES
#Is there parquet counterpart directories for the delta files? If so,
#I propose we eliminate the below. Within the toolbox module, there is a process that reads in the appropiate
#delta files and converts them to parquet. It also does other formating to ensure the data is in the
#necessary format.
parquet_storage_micro_metro_nonmetro = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/geospatial/parquet/metro_micro_nonmetro/'
parquet_storage_state = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/geospatial/parquet/state/'
parquet_storage_media_market = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/geospatial/parquet/media_market/'
parquet_storage_census_region = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/geospatial/parquet/census_region/'
parquet_storage_census_division = 'abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/j468635/geospatial/parquet/census_division/'
