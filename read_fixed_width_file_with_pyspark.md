# Read fixed width file with PySpark

Sometimes we got files in different formats, like a CSV, JSON, or Parquet, files
without separators. In this case, we need to read the file as a fixed-width file.

You must know the structure of the file, so you can use the `read.format("fixed-width")`
method. The structure that I always use is the following:

| Field | Position | Size | Decimals | Type |
| ----- | -------- | ---- | -------- | ---- |
| field_001 | 1 | 10 | | str |
| field_002 | 11 | 10 | 0 | int |
| field_003 | 22 | 10 | 2 | decimal |
| field_004 | 33 | 8 | | date |

If for any reason you don't know the structure of the file, you can get it from
the data base, with a SQL query or with a manually way.

The following code works if you have the structure of the file similar to the CSV
that I showed above. If you have a different structure, you must adapt the code
as you need.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, substring, col

spark = SparkSession.builder.appName("Read Fixed with File with PySpark").getOrCreate()

text_file = spark.read.format("fixed-width").option("trimValues", "true").text("path/to/data.txt")

# Replace null values with empty strings
text_file = text_file.withColumn("value",regexp_replace("value", "\x00", " ")) # Optional

# Load the structure of the fixed-width file
structure = spark.read.csv("path/to/structure.csv", header=True, inferSchema=True)
STRUCT = namedtupe("STRUCT", ["FIELD", "POSITION", "SIZE","DECIMALS", "TYPE"])

struct_df = [
  STRUCT(
    field=field,
    position=int(position),
    size=int(size),
    decimals=int(decimals if str(decimals).replace("None","") else 0),
    type=type_
  )
  for (field, position, size, decimals, type_) in structure.select(
    "FIELD", "POSITION", "SIZE", "DECIMALS", "TYPE"
  ).collect()
]
# Structure the dataframe with the struct data using the size and positions
df = text_file.select(
  *[
    substring(col("value"), field.POSITION, field.SIZE).cast(field.TYPE).alias(field.FIELD)
    for field in struct_df
  ]
)
```
