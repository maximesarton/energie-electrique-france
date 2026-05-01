import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Chargement du dataframe et définition de la valeur 'ND' comme une valeur manquante
df = pd.read_csv('eco2mix-regional-cons-def.csv', sep=';', low_memory=False,
                 na_values=['ND', 'n/a', 'N/A', 'NA', '-'])

# Transformation de colonnes
df['Date'] = pd.to_datetime(df['Date'])
df["Date - Heure"] = pd.to_datetime(df["Date - Heure"], utc=True)
df = df.drop(['Column 30', 'Nature', 'Stockage batterie', 'Déstockage batterie',
              'Eolien terrestre', 'Eolien offshore', 'Heure'], axis=1)

# Sélectionner les années avec TCO% & TCH% (2020-2024)
df = df[df["Date - Heure"] >= pd.Timestamp('2020-01-01', tz='UTC')]

# Remplacer les NaN par 0
cols_to_fill = ["Nucléaire (MW)", "Pompage (MW)", "TCO Nucléaire (%)", "TCH Nucléaire (%)"]
df.loc[:, cols_to_fill] = df[cols_to_fill].fillna(0)

# Export
df.to_csv('eco2mix_clean.csv', index=False, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')
