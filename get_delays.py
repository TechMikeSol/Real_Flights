import psycopg2 
from config import params_dic
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

conn = psycopg2.connect(**params_dic)

# Create a cursor
# Think of a cursor like an object that stores and executes queries
# I prepare it by calling conn.cursor() in Python
# For my purpose, a cursor is an object that allows me to execute queries 
# To learn more about cursors, click here: https://www.geeksforgeeks.org/what-is-cursor-in-sql/
cursor = conn.cursor()

# Execute a query using the cursor
# Docs of cursor here: https://www.psycopg.org/docs/cursor.html
cursor.execute("SELECT * FROM real_flight WHERE cancelled = \'0\' AND diverted = \'0\'")

# Pull all rows from the query you just executed
rows = cursor.fetchall()

# Close the cursor, 
# it must be done once you are done interacting w/ the cursor
cursor.close()

# Save the list of tuples (representing rows) into a dataframe!
df = pd.DataFrame(rows, columns=[desc.name for desc in cursor.description])

# Print out the first 5 rows of the dataframe!
#print(df.head())

# Check if my SQL query only kept rows that have 0

#print(df[df['cancelled'] == "0"])

# Check if I have null or missing values 
# in my dataframe before I clean

missing_rows_dep = df[df["dep_del15"].isna()]
#print(len(missing_rows_dep))

missing_rows_arr = df[df["arr_del15"].isna()]
#print(len(missing_rows_arr))

# I have no missing values but here's the code 
# to drop values anyways below

#clean_df = df.dropna(subset = ['arr_del15', 'dep_del15'])
#print(clean_df)


# Using `pandas`, I'll create a new column within my original dataframe 
# labeled `DELAYED` that will be marked as `1` if either `arr_del15` or `dep_del15` 
# are `True`, and be marked as `0` if both `arr_del15` and `dep_del15` are `False`.

df["delayed"] = np.where((df["arr_del15"] == '1') | (df["dep_del15"] == "1"), 1, 0)


# After creating this column, I'll create a new dataframe that groups 
# each airline (`op_unique_carrier`) into groups and 
# calculates the ratio of delays (`DELAYED`) for each airline.

airline_delays = df.groupby("op_unique_carrier")["delayed"].mean()
airline_delays.sort_values(inplace = True, ascending = False)
#print(airline_delays)

# Group by airline 

airlineid_delays = df.groupby("op_carrier")["delayed"].mean()
airlineid_delays.sort_values(inplace = True, ascending = False)
print(airlineid_delays)

# Group by airport 

origin_delays = df.groupby("origin")["delayed"].mean()
origin_delays.sort_values(inplace = True, ascending = False)
#print(origin_delays)

# Saving my new dataset to a csv

#airline_delays.to_csv("airline_delays_jan.csv")

# Making a simple bar graph from the data

airline_delays.plot.bar()
plt.show()