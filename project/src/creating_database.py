"""
This file creates a database using SQLite and loads the data from the csv file into the database.
"""

import sqlite3
import pandas as pd

# Load the CSV file into a pandas DataFrame
df = pd.read_csv("../data/opioid_by_state_year.csv")

# renaming the last column
df.rename(columns={"sum(DOSAGE_UNIT)": "DOSAGE_UNIT"}, inplace=True)

# Create a connection to the SQLite database
conn = sqlite3.connect("../data/opioid_database.db")

# Write the data to a SQLite table
df.to_sql("opioid_data", conn, if_exists="replace", index=False)

# Execute a SELECT * query
df = pd.read_sql_query("SELECT * FROM opioid_data LIMIT 3", conn)

# Print the DataFrame
print(df)

# Close the connection
conn.close()
