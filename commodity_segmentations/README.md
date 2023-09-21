# Commodity-Segmentations
This process creates sets of UPCs that define commodity-segmentations. Households that purchase any of the UPCs in the set of the given segmentation are included in the segmentation. The UPC set of a commodity-segmentation are pulled from a combination of commodities and sub-commodities. The commodities and sub-commodities are chosen based on how relevant they are to the given segment. For each combination, the most current UPCs are pulled from ACDS/PID to ensure accuracy.

The commodity/sub-commodity combinations and their respective segmentations are stored in a control file.  The location of the live control file is stored in the config.py file.

# Diet Segmentation
This process creates the UPC set for the diet segmentation by pulling UPCs from PIM that contain 'diet' in the product name or product description. Inedible and irrevelant commodities are filtered out from the set.

# Notes
* commodity_segmentations.py is scheduled to run under the autorun_commodity_segmentations Workflow job on Databricks. The job is scheduled to run at 18:00 (UTC-05:00 - Central Time).

* diet_segmentation.py is scheduled to run under the autorun_commodity_segmentations Workflow job on Databricks. The job is scheduled to run at 18:00 (UTC-05:00 - Central Time).
