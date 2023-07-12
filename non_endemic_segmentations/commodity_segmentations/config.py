#Proposal - at storage account sa8451dbxadhocprd and storage container media, let us create
#a new directory called non_endemic_segmentations. Inside of it, we can put commodity_segments and embedded_dimensions.
#That way, all the non-endemic processes live in the save neighborhood.

#Location of segmentations control file live in production
control_fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/commodity_segments/commodity_segments_control.csv"

#Where the commodity-segment's output lands
output_fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/commodity_segments/output/"

#Where the exploratory household analysis lands
hh_counts_fp = "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/commodity_segments/household_counts/"