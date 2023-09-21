# Commodity-Segmentations
This process creates sets of UPCs that define commodity-segmentations. Households that purchase any of the UPCs in the set of the given segmentation are included in the segmentation. The UPC set of a commodity-segmentation are created by choosing a combination of commodities and sub-commodities. The commodities and sub-commodities are chosen based on how relevant they are to the given segment. For each combination, the most current UPCs are pulled from ACDS to ensure accuracy.

The commodity/sub-commodity combinations and their respective segmentations are stored in a control file.  The location of the live control file is stored in the config.py file.

# Diet Segmentation
This process creates the UPC set for the diet segmentation by pulling UPCs that contain 'diet' in the product name or product description.
