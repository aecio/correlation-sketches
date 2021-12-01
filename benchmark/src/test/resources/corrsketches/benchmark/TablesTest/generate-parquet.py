# Script used to generate parquet file used in this test
import pandas as pd

df = pd.DataFrame({
    "key":   list("abc"),
    "value": list(range(1, 4)),
})
df.to_parquet("./test1.parquet", engine="pyarrow")