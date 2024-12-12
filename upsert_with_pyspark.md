# Upsert with PySpark

The upsert operation is a common operation in data warehouses. It is used to update a table with another table, only if the row exists in the first table. Otherwise, it inserts a new row. But since it is not a standard Spark built in function, we need to use a different approach.

## Merge with SQL

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("upsert").getOrCreate()

df_delta = spark.createDataFrame(
    [
        (1, "product 1", 100, 10.0),
        (3, "product 3", 100, 30.0),
        (5, "product 5", 100, 20.0),
        (11, "product 11", 100, 20.0),
    ],
    ["id", "title", "stock", "price"],
)

df_historical = spark.createDataFrame(
    [
        (1, "product 1", 10, 10.0),
        (2, "product 2", 100, 20.0),
        (3, "product 3", 10, 30.0),
        (4, "product 4", 50, 10.0),
        (5, "product 5", 10, 20.0),
        (6, "product 6", 10, 30.0),
        (7, "product 7", 100, 10.0),
        (8, "product 8", 100, 20.0),
        (9, "product 9", 100, 30.0),
        (10, "product 10", 100, 10.0),
    ],
    ["id", "title", "stock", "price"],
)

df_delta.createOrReplaceTempView("delta")
df_historical.createOrReplaceTempView("historical")

keys = [df_historical.columns[0],]
match_cols = [_col for _col in df_historical.columns if _col not in keys]
all_cols = [_col for _col in df_historical.columns]

upsert_df = spark.sql(
    f"""
    MERGE INTO historical h
    USING delta d
    ON h.id = d.id
    WHEN MATCHED THEN
      UPDATE SET {', '.join([f"h.{_col} = d.{_col}" for _col in match_cols])}
    WHEN NOT MATCHED THEN
      INSERT ({', '.join(all_cols)}) VALUES ({', '.join([f"d.{_col}" for _col in all_cols])})
    """
)

```

This is the most common way to do upserts in SQL with PySpark Views. However, if it is not possible to use this method, fell free to use a dataframe join instead.

## Merge with PySpark

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce

spark = SparkSession.builder.appName("upsert").getOrCreate()

df_delta = spark.createDataFrame(
    [
        (1, "product 1", 100, 10.0),
        (3, "product 3", 100, 30.0),
        (5, "product 5", 100, 20.0),
        (11, "product 11", 100, 20.0),
    ],
    ["id", "title", "stock", "price"],
)

df_historical = spark.createDataFrame(
    [
        (1, "product 1", 10, 10.0),
        (2, "product 2", 50, 20.0),
        (3, "product 3", 10, 30.0),
        (4, "product 4", 50, 10.0),
        (5, "product 5", 10, 20.0),
        (6, "product 6", 50, 30.0),
        (7, "product 7", 50, 10.0),
        (8, "product 8", 50, 20.0),
        (9, "product 9", 50, 30.0),
        (10, "product 10", 100, 10.0),
    ],
    ["id", "title", "stock", "price"],
)
keys = [df_historical.columns[0],]

df_upsert = df_historical.join(
  other=df_delta,
  on=keys
  how='full_outer'
)

columns_to_update = [_col for _col in df_historical.columns if _col not in keys]
upsert_columns = [
  coalesce(df_delta[_col], df_historical[_col]).alias(_col) for _col in columns_to_update
]

df_upsert = df_upsert.select(*keys, *upsert_columns)

```

## How it works

Think in a table A and a table B.

A

| Id | Title | Stock | Price |
| --- | --- | --- | --- |
| 1 | Product 1 | 10 | 10.0 |
| 2 | Product 2 | 50 | 20.0 |
| 3 | Product 3 | 10 | 30.0 |
| 4 | Product 4 | 50 | 10.0 |
| 5 | Product 5 | 10 | 20.0 |
| 6 | Product 6 | 50 | 30.0 |
| 7 | Product 7 | 50 | 10.0 |
| 8 | Product 8 | 50 | 20.0 |
| 9 | Product 9 | 50 | 30.0 |
| 10 | Product 10 | 50 | 10.0 |

B

| Id | Title | Stock | Price |
| --- | --- | --- | --- |
| 1 | Product 1 | 100 | 10.0 |
| 3 | Product 3 | 100 | 30.0 |
| 5 | Product 5 | 100 | 20.0 |
| 11 | Product 11 | 100 | 20.0 |

And if we want to update table A with table B.

The expected result is:

| Id | Title | Stock | Price |
| --- | --- | --- | --- |
| 1 | Product 1 | 100 | 10.0 |
| 2 | Product 2 | 50 | 20.0 |
| 3 | Product 3 | 100 | 30.0 |
| 4 | Product 4 | 50 | 10.0 |
| 5 | Product 5 | 100 | 20.0 |
| 6 | Product 6 | 50 | 30.0 |
| 7 | Product 7 | 50 | 10.0 |
| 8 | Product 8 | 50 | 20.0 |
| 9 | Product 9 | 50 | 30.0 |
| 10 | Product 10 | 50 | 10.0 |
| 11 | Product 11 | 100 | 20.0 |

With the `full_outer` join statement, we can get the following result:

| Id | Title | Stock | Price | Title | Stock | Price |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Product 1 | 10 | 10.0 | Product 1 | 100 | 10.0 |
| 2 | Product 2 | 50 | 20.0 | NULL | NULL | NULL|
| 3 | Product 3 | 10 | 30.0 | Product 3 | 100 | 30.0 |
| 4 | Product 4 | 50 | 10.0 | NULL | NULL | NULL |
| 5 | Product 5 | 10 | 20.0 | Product 5 | 100 | 20.0 |
| 6 | Product 6 | 50 | 30.0 | NULL | NULL | NULL |
| 7 | Product 7 | 50 | 10.0 | NULL | NULL | NULL |
| 8 | Product 8 | 50 | 20.0 | NULL | NULL | NULL |
| 9 | Product 9 | 50 | 30.0 | NULL | NULL | NULL |
| 10 | Product 10 | 100 | 10.0 | NULL | NULL | NULL |
| 11 | NULL | NULL | NULL | Product 11 | 100 | 20.0 |

Finally we can use the `coalesce` function to get the expected result.

| Id | Title | Stock | Price |
| --- | --- | --- | --- |
| 1 | Product 1 | 100 | 10.0 |
| 2 | Product 2 | 50 | 20.0 |
| 3 | Product 3 | 100 | 30.0 |
| 4 | Product 4 | 50 | 10.0 |
| 5 | Product 5 | 100 | 20.0 |
| 6 | Product 6 | 50 | 30.0 |
| 7 | Product 7 | 50 | 10.0 |
| 8 | Product 8 | 50 | 20.0 |
| 9 | Product 9 | 50 | 30.0 |
| 10 | Product 10 | 50 | 10.0 |
| 11 | Product 11 | 100 | 20.0 |

## Conclusion

Both methods are valid, use whichever you prefer. SQL developers could prefer the `MERGE` statement, while Python developers could prefer the `join` statement.
