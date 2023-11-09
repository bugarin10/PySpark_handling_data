"""
This file extracts the data from the Washington Post website, transforms it, and loads it into a csv file.
"""


import urllib.request
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Define the file path
url = "https://gfx-data.news-engineering.aws.wapo.pub/ne-static/arcos/v2/bulk/arcos_all_washpost.zip"
file_path = "/tmp/arcos_all_washpost.tsv"

# Download the .zip file
zip_path, _ = urllib.request.urlretrieve(url, "/tmp/arcos_all_washpost.zip")

# Extract the .tsv file
with zipfile.ZipFile(zip_path, "r") as zip_ref:
    zip_ref.extractall("/tmp")

# Read the data
df = spark.read.csv(file_path, sep="\t", header=True)
print("table loaded")
# Changing dosage unit to float
df = df.withColumn("DOSAGE_UNIT", col("DOSAGE_UNIT").cast("float"))
# Changing transaction date to int
df = df.withColumn("YEAR", col("TRANSACTION_DATE").substr(1, 4).cast("int"))
# Groupingby
df_grouped = df.groupBy("REPORTER_STATE", "BUYER_STATE", "DRUG_NAME", "YEAR").sum(
    "DOSAGE_UNIT"
)
# Converting to pandas dataframe
pandas_df = df_grouped.toPandas()
# Exporting the data
pandas_df.to_csv("../data/opioid_by_state_year.csv", index=False)
