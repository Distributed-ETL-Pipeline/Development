# %% 
import pandas as pd
import glob
import duckdb
import time



# %%
connection = duckdb.connect("testDB.db")
# %%
cur_time = time.time()
df = pd.concat([pd.read_csv(f) for f in glob.glob("Dataset/Sales_Product_Combined.csv")])
print("Time to read CSVs: ", time.time() - cur_time)
print(df.head(10))
# %%
cur_time = time.time()
df = connection.execute("""
    SELECT * FROM read_csv_auto('Dataset/Sales_Product_Combined.csv', header=True, filename=True)
""").df()
# df() fetchdf() arrow()
print("Time to read CSVs: ", time.time() - cur_time)
print(df)


# %%
connection.register("df_view", df)
connection.execute("DESCRIBE df_view").df()
# %%
connection.execute("""
SELECT COUNT(*) FROM df_view""").df()
# %%
df.isnull().sum()
# %%
