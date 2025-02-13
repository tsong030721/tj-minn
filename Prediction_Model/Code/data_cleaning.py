import pandas as pd

df_pbp = pd.read_csv("../Data/csv/play_by_play.csv", nrows = 100)

print(df_pbp.columns)
print(df_pbp.dtypes)
