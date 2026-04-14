import pandas as pd
from pathlib import Path

df = pd.read_csv(r"C:\Users\caro_\OneDrive\Desktop\UP\1OCTAVO\datosMasivos\casino_bigdata\data\raw\online_casino_games_dataset_v2.csv", low_memory=False)

print(df.isnull().sum())
print("\nTotal filas:", len(df))