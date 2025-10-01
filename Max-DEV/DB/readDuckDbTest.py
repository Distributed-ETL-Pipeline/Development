import duckdb
import pandas as pd
from duckDbTest2 import connect_to_db, query_db

if __name__ == "__main__":
    conn = duckdb.connect("testDB.db", read_only=True)

    result = query_db(conn, """
                      SELECT DISTINCT Product 
                      FROM Sales_Product_Combined 
                      WHERE TRIM(LOWER(City)) = 'dallas';
                      """)
    print(result)

    df = pd.DataFrame(result, columns=["Product"])
    print(df)

    for product in result:
        print(f"{product[0]}")  # Each product is a tuple, e.g., ('ProductA',)


    result = query_db(conn, """
                      SELECT * FROM Sales_Product_Combined
                      """, df=True)
    print(result)

    result = query_db(conn, """
                      SELECT * FROM Sales_Product_Combined 
                      WHERE CAST(REPLACE(Price, ',', '') AS DOUBLE) > 200
                      AND Product LIKE '%Headphones%';
                      """, df=True)
    print(result)

    result = query_db(conn, """
                      SELECT * FROM Sales_Product_Combined 
                      WHERE Product LIKE '%Headphones%';
                      """, df=True)
    print(result)
