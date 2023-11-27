# Commodity-Segmentations
Creates lists of UPCs that define commodity-segmentations. The UPC list of a commodity-segmentation are pulled from a combination of commodities and sub-commodities. The commodities and sub-commodities are chosen based on how relevant they are to the given segment. For each combination, the most current UPCs are pulled from ACDS and PID to ensure accuracy. The commodity/sub-commodity combinations and their respective segmentations are stored in the **commodity_segmentations** dictionary object in config.py.

Each of these UPC list are used to define the evaluation universe for each segmentation. In a separate module, this universe of households are given a propensity ranking depending on how much they spent on the given UPC list. This propensity ranking is typically done via fun-lo, percentiles, or a similar method to rank the households. The value of propensity ranking is to improve the accuracy of our segmentations' household compositions.

# Fuel Segmenations
Creates segmentations based on fuel type purchases. Households are ranked L if the gallons of fuel they purchased is < the 33rd percentile of fuel purchasers, they are ranked H if they purchased > the 66th percentile, and they are ranked M if they purchased <= the 66th percentile and purchased >= the 33rd percentile. This ranking is done for each fuel type. Since third party fuel does not distinguish between fuel types, third party fuel is not considered in any of the fuel type segmentations.

A segmentation is also created for all-inclusive fuel purchases (not distinguishing between fuel types). This households in this segmentation are given L, M, and H rankings following the same methodology for fuel types. The value of this segmentation is that it includes third party fuel purchases.

# Percentile Segmentations
Creates segmentations by ranking households on how much they spent on the given UPC list. Households are ranked L if the amount of dollars spent on the UPC list is < the 33rd percentile of the amount of dollars spent on the UPC list, they are ranked H if they spent > the 66th percentile, and they are ranked M if they spent <= the 66th percentile and spent >= the 33rd percentile.

The segmentations that go through this methodology are stored in the **percentile_segmentations** list object in config.py.

# Diet Segmentation
~~Creates the UPC set for the diet segmentation by pulling UPCs from PIM that contain 'diet' in the product name or product description. Inedible and irrevelant commodities are filtered out from the set. (REJECTED BY LEGAL BECAUSE 'DIET' IS TOO CLOSE TO A MEDICAL CONDITION OR DIAGNOSIS)~~

# Notes
* commodity_segmentations.py is scheduled to run under the autorun_commodity_segmentations Workflow job on Databricks. The job is scheduled to run at 00:00 (UTC-05:00 - Central Time) every Friday.

* fuel_segmentations.py is scheduled to run under the autorun_commodity_segmentations Workflow job on Databricks. The job is scheduled to run at 00:00 (UTC-05:00 - Central Time) every Friday.

* percentile_segmentations.py is scheduled to run under the autorun_commodity_segmentations Workflow job on Databricks. The job is scheduled to run upon the completion of commodity_segmentations.py.
