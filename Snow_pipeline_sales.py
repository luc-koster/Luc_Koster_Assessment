import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd


print("opening..")

# Extracting data of all the sales of 2019.

df = pd.read_csv("all_data.csv", sep=",", header=0)


# Transforming the data by dropping all the NaN values as well as the OR values. 
# Which got mixed in when concatenating all 12 monthly files into 1 yearly file.
# Converted the "Quantity Order" and "Price Each" to numeric values in order to create the "Sales" column.
# Created a "City" Column by taking the 2nd value of the address column including the state.
# Created a "Hour" and "Minute" column to make advertising times easier.

def get_city(address):
    return address.split(",")[1]

def get_state(address):
    return address.split(",")[2].split(" ")[1]


nan_df = df[df.isna().any(axis=1)]
df = df.dropna(how="all")
df = df[df["Order Date"].str[0:2] != "Or"]
df["Month"] = df["Order Date"].str[0:2]
df["Month"] = df["Month"].astype("int32")
df["Quantity Ordered"] = pd.to_numeric(df["Quantity Ordered"])
df["Price Each"] = pd.to_numeric(df["Price Each"])
df["Sales"] = df["Quantity Ordered"] * df["Price Each"]
df["City"] = df["Purchase Address"].apply(lambda x: f"{get_city(x)} ({get_state(x)})")
df["Order Date"] = pd.to_datetime(df["Order Date"])
df["Hour"] = df["Order Date"].dt.hour
df["Minute"] = df["Order Date"].dt.minute



print(df.head(10))
print("opening snowflake..")

# Loading the transformed files into snowflake for storage.

cnn = snowflake.connector.connect(
    user="LUCKOSTER",
    password="Klootzak132!",
    account="EPORRXE-WU15947",
    warehouse="project_warehouse",
    database="project_database",
    schema="project_schema"
    )
success, nchunks, nrows, _ = write_pandas(cnn, df, "PROJECT_SALES", auto_create_table=True, overwrite=True)
print(str(success) + ", " + str(nchunks) + ", " + str(nrows))
cnn.close()
print("done.")


