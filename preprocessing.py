# ============================================
# ETL — Chargement et nettoyage initial
# ============================================

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Chargement du dataframe et définition de la valeur 'ND' comme une valeur manquante
df = pd.read_csv('../data/eco2mix-regional-cons-def.csv', sep=';', low_memory=False,
                 na_values=['ND', 'n/a', 'N/A', 'NA', '-'])

# Transformation de colonnes (mauvais type + intégralement vides)
df['Date'] = pd.to_datetime(df['Date'])
df["Date - Heure"] = pd.to_datetime(df["Date - Heure"], utc=True)
df = df.drop(['Column 30', 'Nature', 'Stockage batterie', 'Déstockage batterie',
              'Eolien terrestre', 'Eolien offshore', 'Heure'], axis=1)
df.dtypes

# Sélectionner les années avec TCO% & TCH% (2020-2024)
df = df[df["Date - Heure"] >= pd.Timestamp('2020-01-01', tz='UTC')]

# Remplacer les NaN par 0
cols_to_fill = ["Nucléaire (MW)", "Pompage (MW)", "TCO Nucléaire (%)", "TCH Nucléaire (%)"]
df.loc[:, cols_to_fill] = df[cols_to_fill].fillna(0)

# Export CSV pour PowerBI
df.to_csv('../data/eco2mix_clean.csv', index=False, encoding='utf-8',
          date_format='%Y-%m-%d %H:%M:%S')


# ============================================
# Analyse des valeurs manquantes par année
# ============================================

def analyze_missing_and_zeros(df, year):
    """Analyze NaN and zero percentages for a given year"""
    df_year = df[df['Date'].dt.year == year]

    # NaN analysis (all columns)
    nan_pct = (df_year.isna().sum() / len(df_year) * 100).round(2)

    # Zero analysis (numeric columns only)
    numeric_cols = df_year.select_dtypes(include=['float64', 'int64']).columns
    zero_pct = ((df_year[numeric_cols] == 0) & (~df_year[numeric_cols].isna())).sum() / len(df_year) * 100
    zero_pct = zero_pct.round(2)

    return pd.DataFrame({
        f'NaN_%_{year}': nan_pct,
        f'Zero_%_{year}': zero_pct
    })

# Get all unique years
years = sorted(df['Date'].dt.year.unique())

# Region check
dfregion = df[df['Région'] == 'Bretagne']

# Replace check
dfreplace = df[['Stockage batterie', 'Déstockage batterie',
                'Eolien terrestre', 'Eolien offshore', 'Date']]

# L'analyse des NaNs par région montre qu'ils sont tous "logiques"
# (100% NaNs sur une année sur une région) et correspondent à une absence de production

# Create a combined DataFrame for all years
combined_analysis = pd.concat([analyze_missing_and_zeros(dfreplace, year) for year in years], axis=1)

print("Analysis of NaN and Zero values by year:")
combined_analysis


# ============================================
# Analyse des colonnes vides par région
# ============================================

def analyze_vide_columns_by_region(df):
    """
    Identify columns that have 100% NaN, 100% zeros, or combined NaN+zeros = 100%
    Grouped by year AND region
    """
    years = sorted(df['Date'].dt.year.unique())
    regions = sorted(df['Région'].unique())

    all_results = []

    for year in years:
        for region in regions:
            df_subset = df[(df['Date'].dt.year == year) & (df['Région'] == region)]

            if len(df_subset) == 0:
                continue

            nan_pct = (df_subset.isna().sum() / len(df_subset) * 100).round(2)

            numeric_cols = df_subset.select_dtypes(include=['float64', 'int64']).columns
            zero_pct = ((df_subset[numeric_cols] == 0) &
                        (~df_subset[numeric_cols].isna())).sum() / len(df_subset) * 100
            zero_pct = zero_pct.round(2)

            result = pd.DataFrame({
                'Year': year,
                'Région': region,
                'Column': nan_pct.index,
                'NaN_%': nan_pct.values,
                'Zero_%': zero_pct.reindex(nan_pct.index, fill_value=0).values
            })

            result['Combined_%'] = result['NaN_%'] + result['Zero_%']
            result['Status'] = result['Combined_%'].apply(
                lambda x: 'Vide' if x >= 99.99 else 'Pas vide'
            )

            all_results.append(result)

    return pd.concat(all_results, ignore_index=True)

# Run the analysis
vide_analysis_by_region = analyze_vide_columns_by_region(df)

print("Columns completely empty by year and region:")
vide_analysis_by_region[vide_analysis_by_region['Status'] == 'Vide']


# ============================================
# Colonnes partiellement vides
# (étape intermédiaire — utile pour l'exploration)
# ============================================

vide_partiel_NaN = vide_analysis_by_region[vide_analysis_by_region["NaN_%"] > 0]\
    .reset_index(drop=True)\
    .drop(["Zero_%", "Combined_%", 'Status'], axis=1)

vide_partiel_Zero = vide_analysis_by_region[vide_analysis_by_region["Zero_%"] > 50]\
    .reset_index(drop=True)\
    .drop(["NaN_%", "Combined_%", 'Status'], axis=1)

print("NaN: ", vide_partiel_NaN.shape, "Zero: ", vide_partiel_Zero.shape)
# Les valeurs de vide_partiel_Zero sont sur des colonnes Pompage — comportement attendu
vide_partiel_NaN


# ============================================
# Mapping des colonnes vides — Heatmap
# ============================================

# Pivot table pour la heatmap
heatmap_data = vide_analysis_by_region.pivot_table(
    index=['Year', 'Région'],
    columns='Column',
    values='Combined_%',
    fill_value=0
)

# Version binaire (Vide = 1, sinon 0)
binary_data = (heatmap_data >= 99.99).astype(int)

# Heatmap
plt.figure(figsize=(20, 12))
sns.heatmap(binary_data,
            cmap=['lightgray', 'red'],
            cbar_kws={'ticks': [0, 1], 'label': 'Empty (Vide)'},
            linewidths=0.5,
            linecolor='lightgray')
plt.title('Columns Completely Empty by Year and Region', fontsize=16)
plt.xlabel('Columns')
plt.ylabel('Year - Region')
plt.tight_layout()
plt.show()
