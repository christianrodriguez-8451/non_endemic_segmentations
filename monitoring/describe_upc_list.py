# Databricks notebook source
"""
Conducts exploratory data analysis on the
inputted UPC list. The code assumes the UPC list
is PySpark dataframe with "gtin_no" as the column that
contains the UPCs. The code provides the following:

  1) Images of the UPC taken from Kroger.com.

  2) Product name, product description, department,
  sub-department, micro-department, commodity, and 
  sub-commodity information of those image UPCs.

  3) Top 100 words when all product names are considered.

  4) Top 100 words when all product descriptions are
  considered.

  5) Grouped-by counts at the department, sub-department,
  micro-department, commodity, and sub-commodity levels.

To do:
  Add product brand in counts analysis.
"""

# COMMAND ----------

from PIL import Image
import requests
import io
import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import commodity_segmentations.config as con
import matplotlib.pyplot as plt
import random
import resources.config as config
import seaborn as sns
import os

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)

def get_image_size(url):
  """ Returns size of the image located at the inputted URL.

  Example
  ----------
    get_image_size('https://www.kroger.com/product/images/medium/front/0019264047717')

  Parameters
  ----------
  url: str
    URL string of where the image is located.

  Returns
  ----------
  A two-dimension tuple with the size image or None if there is no
  image at the inputted URL.
  """
  try:
      # Download the image from the URL
      response = requests.get(url)
      response.raise_for_status()  # Raise an exception for 4xx and 5xx status codes

      # Open the image using Pillow
      image_data = io.BytesIO(response.content)
      image = Image.open(image_data)

      # Get the size of the image
      width, height = image.size

      return (width, height)
  except Exception as e:
      print("Error:", e)
      return None

def draw_nonempty_upcs(df, n=25, upc_colname = 'gtin_no'):
  """Randomly draws UPCs and checks if they have an image
  at https://www.kroger.com/product/images/medium/front/.
  Re-draws the UPC if there is no image found for that UPC.

  Example
  ----------
    draw_nonempty_upcs(df, 25, 'gtin_no')

  Parameters
  ----------
  df: pyspark.sql.dataframe.DataFrame
    PySpark dataframe that has a column with UPC codes.

  n: int
    Integer that defines how many UPCs to draw.

  upc_colname: str
    String that defines the column name in df that contains the
    UPC values. 

  Returns
  ----------
  A list that contains the randomly drawn UPC codes that are confirmed
  to have an image at https://www.kroger.com/product/images/medium/front/.
  """
  #We do a single collect of 50000 randomly drawn UPCs
  #store in a list and cut down on processing time during the re-draws.
  upc_bank = df.\
    select(upc_colname).\
    sample(False, 0.1).\
    limit(50000).\
    rdd.map(lambda x: x[0]).\
    collect()

  #Initially draw of UPCs  
  random_upcs = random.sample(upc_bank, n)
  
  url_fmt = "https://www.kroger.com/product/images/medium/front/{}"
  bad_upcs = []
  good_upcs = []
  dont_redraw = []
  redrawn_upcs = random_upcs
  while len(redrawn_upcs) != 0:
    #For each UPC, check if they have no image
    for upc in redrawn_upcs:
      url = url_fmt.format(upc)
      size = get_image_size(url)

      #If there is no image, then add them to bad UPC list for re-draw
      if size is None:
        print("{} is a bad UPC!".format(upc))
        bad_upcs += [upc]
        dont_redraw += [upc]

      #Is there is an image, then add them to good UPC list for the output
      else:
        good_upcs += [upc]
        dont_redraw += [upc]

    #Drop the UPCs that do not have an image and re-draw from the dataframe
    if len(bad_upcs) != 0:
      print("Executing re-draw...")

      #Filter out the UPCs we have already drawn
      [upc_bank.remove(d) for d in dont_redraw]
      #Define n that we have to re-draw
      redraw_n = len(bad_upcs)
      #Reset bad UPC list for the next iteration
      bad_upcs = []
      #Reset the don't-redraw deck since we already dropped those from the bank
      dont_redraw = []

      #Do the re-draw
      if len(upc_bank) != 0:
        redrawn_upcs = random.sample(upc_bank, redraw_n)

      #Ran out of UPCs. There is nothing to re-draw from.
      else:
        redrawn_upcs = []
        print("Could not redraw any more UPCs. Ran out of UPCs.")
    
    #There is no bad UPCs to re-draw
    else:
      redrawn_upcs = []

  #We tried re-drawing as much as possible - no UPC had an image to show
  if len(good_upcs) == 0:
    raise ValueError("No UPC in the list has an image at www.kroger.com.")
  #We tried re-drawing, but could only get so many images
  elif len(good_upcs) > 0 and len(good_upcs) < n:
    print("Only {} UPCs had an image at www.kroger.com.")

  return(good_upcs)

def display_upcs(upcs):
  """Returns an image from https://www.kroger.com/product/images/medium/front/
  for each UPC in the inputted list.

  Example
  ----------
    display_upcs(['0082778285046', '0007248921915', '0081235026102'])

  Parameters
  ----------
  upcs: list
    List of strings where each string is a UPC code. UPC codes
    need to be string since leading zeroes matter.

  Returns
  ----------
  A pandas.io.formats.style.Styler object that has an image
  for each inputted UPC.
  """
  ldf=pd.DataFrame(upcs,columns=['UPC'])
  ldf = ldf.sort_values(by='UPC', ascending=False)
  ldf = ldf.reset_index(drop=True)
  ldf['IMG']=ldf['UPC'].map(lambda x: f'<img src="https://www.kroger.com/product/images/medium/front/{x}">' )#if x.isnumeric() else '')
  return ldf.T.style

def top_words(df, string_col, n=100):
  """Returns the top n words for the inputted column
  in df.

  Example
  ----------
    top_words(df, 'product_name', 100)

  Parameters
  ----------
  df: pyspark.sql.dataframe.DataFrame
    PySpark dataframe that has the string column we'd
    like to extract the top n words from.

  string_col: str
    String that the defines the column name of the 
    column of interest. 

  n: int
    Integer that defines the top n cut-off.

  Returns
  ----------
  A list where the first element is the most frequent word and the
  last element is the n-th most frequent word. Each element is formatted
  as '{word} ({count})' where the parentheses contains the count
  of how many times that word occured when considering all values 
  in the given column. 
  """
  #Get the complete word occurence in each commodity/sub-commodity
  word_count = df.select(f.explode(f.split(df[string_col], " ")).alias("word"))
  word_count = word_count.withColumn("word", f.regexp_replace("word", ",", ""))
  word_count = word_count.withColumn("length", f.length("word"))
  word_count = word_count.filter(f.col("length") > 0)
  word_count = word_count.groupBy(["word"]).count()
  windowSpec = Window.orderBy(f.desc("count"))
  word_count = word_count.withColumn("row_index", f.row_number().over(windowSpec))

  #Collect the top n words and output them as a list since PySpark only
  #shows a maximum of 50 rows
  word_count = word_count.filter(f.col("row_index") <= n)
  words = word_count.select("word").collect()
  words = [row[0] for row in words]
  counts = word_count.select("count").collect()
  counts = [row[0] for row in counts]
  top_words = ["{} ({})".format(w, c) for w, c in zip(words, counts)]
  
  return(top_words)

def display_counts(df, col, limit=15):
  """Returns the grouped-by counts for the inputted
  column and visualizes the counts via a bar chart.

  Example
  ----------
    display_counts(df, "department", 15)

  Parameters
  ----------
  df: pyspark.sql.dataframe.DataFrame
    PySpark dataframe contains the data we'd like
    to conduct the group-by count on.

  col: str
    String that the defines the column name of the 
    column of interest. 

  limit: int
    Integer that defines the the cut-off of how many
    bars to plot in the bar chart. Reduces cluttering.

  Returns
  ----------
  None. Counts and visual are displayed as cell output.
  """
  #Get counts - organize from most dominant to least
  counts_df = df.\
    groupBy(col).\
    count().\
    orderBy('count', ascending=False)
  #Display counts of top 50
  counts_df.show(50, truncate=False)

  #Limit the bar plot to top 15 to avoid overcluttering
  windowSpec = Window.orderBy(f.desc("count"))
  counts_df = counts_df.withColumn("row_index", f.row_number().over(windowSpec))
  counts_df = counts_df.filter(f.col("row_index") <= limit)

  # Convert Spark DataFrame to Pandas DataFrame for plotting
  counts_df = counts_df.toPandas()

  # Plotting
  plt.figure(figsize=(10, 6))
  plt.bar(counts_df[col], counts_df['count'])
  plt.xlabel(col)
  plt.ylabel('Counts')
  plt.title('Counts by {}'.format(col))
  plt.xticks(rotation=90)  # Rotate x-axis labels for better readability
  plt.tight_layout()  # Adjust layout to prevent clipping of labels
  plt.show()

def display_histplot(df, col):
  """Visualizes a histogram for the inputted
  column in the given dataframe.

  Example
  ----------
    display_histplot(df, "dot_product")

  Parameters
  ----------
  df: pyspark.sql.dataframe.DataFrame
    PySpark dataframe contains the data we'd like
    to conduct the group-by count on.

  col: str
    String that the defines the column name of the 
    column of interest.

  Returns
  ----------
  None. Visual is displayed as cell output.
  """

  pandas_df = df.toPandas()

  plt.figure(figsize=(10, 6))
  sns.histplot(pandas_df[col], kde=True)
  plt.title("Distribution of {}".format(col))
  plt.xlabel("Values")
  plt.ylabel("Frequency")
  plt.show()

def write_out(df, fp, delim=",", fmt="csv"):
  """Writes out PySpark dataframe as a csv file
  that can be downloaded for Azure and loaded into
  Excel very easily.

  Example
  ----------
    write_out(df, "abfss://media@sa8451dbxadhocprd.dfs.core.windows.net/audience_factory/adhoc/data_analysis.csv")

  Parameters
  ----------
  df: pyspark.sql.dataframe.DataFrame
    PySpark dataframe contains the data we'd like
    to conduct the group-by count on.

  fp: str
    String that the defines the column name of the 
    column of interest.

  delim: str
    String that specifies which delimiter to use in the
    write-out. Default value is ','.

  fmt: str
    String that specifies which format to use in the
    write-out. Default value is 'csv'.

  Returns
  ----------
  None. File is written out specified Azure location.
  """
  #Placeholder filepath for intermediate processing
  temp_target = os.path.dirname(fp) + "/" + "temp"

  #Write out df as partitioned file. Write out ^ delimited
  df.coalesce(1).write.options(header=True, delimiter=delim).mode("overwrite").format(fmt).save(temp_target)

  #Copy desired file from parititioned directory to desired location
  temporary_fp = os.path.join(temp_target, dbutils.fs.ls(temp_target)[3][1])
  dbutils.fs.cp(temporary_fp, fp)
  dbutils.fs.rm(temp_target, recurse=True)


# COMMAND ----------

#Get latest UPC file
#This chunk is the most important - as long as you
#read in upc lists as a PySpark dataframe with gtin_no
#as the column for the UPCs, then the rest of the code should
#work fine.
s = "fitness_enthusiast"
seg_dir = f"{con.output_fp}upc_lists/{s}/"
dir_contents = dbutils.fs.ls(seg_dir)
dir_contents = [x[0] for x in dir_contents if s in x[1]]
dir_contents.sort()
seg_fp = dir_contents[-1]
df = spark.read.format("delta").load(seg_fp)

df = df.select("gtin_no")
#df.show(50, truncate=False)
df.count()

# COMMAND ----------

n = 25
image_upcs = draw_nonempty_upcs(df, n)
display_upcs(image_upcs)

# COMMAND ----------

#Read in PIM and keep product name, product description, commodity,
#sub-commodity, department, and sub-department.
#Add brand?
pim_fp = config.get_latest_modified_directory(config.azure_pim_core_by_cycle)
pim = config.spark.read.parquet(pim_fp)
pim = pim.select(
  f.col("upc").alias("gtin_no"),
  f.col("gtinName").alias("product_name"),
  f.col("krogerOwnedEcommerceDescription").alias("product_description"),
  f.col("familyTree.commodity.name").alias("commodity"),
  f.col("familyTree.subCommodity.name").alias("sub_commodity"),
  f.col("familyTree.primaryDepartment.name").alias("department"),
  f.col("familyTree.primaryDepartment.recapDepartment.name").alias("sub_department"),
  f.col("familyTree.primaryDepartment.recapDepartment.department.name").alias("micro_department"),
)
pim = pim.select(
  "gtin_no", "product_name", "product_description",
  "department", "sub_department", "micro_department",
  "commodity", "sub_commodity",
)
pim = pim.dropDuplicates(["gtin_no"])

#Join with upc lists for analysis down the road
df = df.join(pim, "gtin_no")
#comms = []
#scomms = ["FMB FLAVORS", "HARD SELTZER/WATER"]
#df = pim.filter((f.col("commodity").isin(comms)|f.col("sub_commodity").isin(scomms)))

#Print the info of the image UPCs for more context
image_df = df.filter(f.col("gtin_no").isin(image_upcs))
image_df = image_df.select("gtin_no", "product_description", "commodity", "sub_commodity")
image_df = image_df.orderBy(f.desc("gtin_no"))
image_df.show(50, truncate=False)

df.count()

# COMMAND ----------

product_name_tw = top_words(df, "product_name")
print(product_name_tw)

# COMMAND ----------

product_desc_tw = top_words(df, "product_description")
print(product_desc_tw)

# COMMAND ----------

#Get UPC counts by product name, product description, commodity,
#sub-commodity, department, and sub-department.
display_counts(df, "department")

# COMMAND ----------

display_counts(df, "sub_department")

# COMMAND ----------

display_counts(df, "micro_department")

# COMMAND ----------

display_counts(df, "commodity")

# COMMAND ----------

display_counts(df, "sub_commodity")

# COMMAND ----------

#Code chunk used to provide example products for marketing
for k in con.sensitive_segmentations.keys():
  comms = con.sensitive_segmentations[k]["commodities"]
  scomms = con.sensitive_segmentations[k]["sub_commodities"]
  #comms = []
  #scomms = ["FMB FLAVORS", "HARD SELTZER/WATER"]

  filtered_df = pim.filter((f.col("commodity").isin(comms)|f.col("sub_commodity").isin(scomms)))
  upcs = filtered_df.select("product_description").limit(10).collect()
  upcs = [row.product_description for row in upcs]
  print(k + ": " + ', '.join(upcs) + "\n\n")

