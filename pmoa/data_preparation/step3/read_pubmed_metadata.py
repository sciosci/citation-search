import pandas as pd

pd.set_option('display.max_columns', None)

df = pd.read_parquet('pubmed_metadata_20231204.parquet')

print(df.head())
