import pandas as pd

# Beispiel DataFrame A
data_a = {'adsh': [1, 2, 3], 'form': ['10-K', '10-Q', '10-K'], 'fp': ['FY', 'Q1', 'FY']}
df_a = pd.DataFrame(data_a)
data_b = {'adsh': [2, 3, 1, 1, 1, 2, 2, 3, 3],
          'qtrs': [1, 2, 4, 3, 4, 1, 2, 3, 4],
          'value': [10, 20, 30, 40, 50, 60, 70, 80, 90]}
df_b = pd.DataFrame(data_b)

# Temporäres DataFrame für "form" hinzufügen
temp_df_b = pd.merge(df_b, df_a[['adsh', 'form']], on='adsh', how='inner')

# Filterkriterien
criteria = (
        ((temp_df_b['form'] == '10-K') & (temp_df_b['qtrs'] == 4)) |
        ((temp_df_b['form'] == '10-Q') & (temp_df_b['qtrs'] == 1))
)

# Ergebnis DataFrame B filtern
result_df = temp_df_b[criteria]
del result_df['form']

print(result_df)