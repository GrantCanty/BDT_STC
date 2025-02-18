import pandas as pd
from collections import defaultdict
import numpy as np

# Carica il file CSV in un DataFrame
df = pd.read_csv('scaled_preds.csv')

# Lista delle colonne da sommare
columns_to_sum = ['refuse', 'paper', 'mgp', 'res_organic', 'school_organic', 'leaves', 'xmas']

# Crea la nuova colonna 'sum' con la somma delle colonne specificate
df['sum'] = df[columns_to_sum].sum(axis=1)

# Salva il DataFrame modificato in un nuovo file CSV
df.to_csv('scaled_preds_with_sum.csv', index=False)

print("Colonna 'sum' creata e file salvato come 'scaled_preds_with_sum.csv'.")


import pandas as pd
from collections import defaultdict
import numpy as np

# Carica il file CSV in un DataFrame
df = pd.read_csv('scaled_preds_with_sum.csv')

# Dizionario per memorizzare i vettori
vectors = defaultdict(list)

# Itera sulle righe del DataFrame
for index, row in df.iterrows():
    # Crea una chiave unica combinando borough_id e community_district
    key = (row['borough_id'], row['community_district'])
    
    # Aggiungi il valore della colonna 'sum' al vettore corrispondente
    vectors[key].append(row['sum'])

# Funzione per calcolare il coefficiente angolare (pendenza) della retta di regressione
def calculate_slope(vector):
    x = np.arange(len(vector))  # Asse x: indice dei punti (0, 1, 2, ...)
    y = np.array(vector)        # Asse y: valori del vettore
    slope, _ = np.polyfit(x, y, 1)  # Calcola la pendenza (coefficiente angolare)
    return slope

# Dizionario per memorizzare le pendenze
slopes = {}

# Calcola la pendenza per ciascun vettore
for key, vector in vectors.items():
    slope = calculate_slope(vector)
    slopes[key] = slope
    print(f"Pendenza per borough_id={key[0]} e community_district={key[1]}: {slope:.4f}")

# Converti il dizionario delle pendenze in un DataFrame
slopes_df = pd.DataFrame(list(slopes.items()), columns=['Key', 'slope'])

# Separa borough_id e community_district
slopes_df[['borough_id', 'community_district']] = pd.DataFrame(slopes_df['Key'].tolist(), index=slopes_df.index)
slopes_df = slopes_df.drop(columns=['Key'])

# Salva il DataFrame in un file CSV
slopes_df.to_csv('slopes.csv', index=False)

import pandas as pd

# Carica il file CSV delle pendenze
slopes_df = pd.read_csv('slopes.csv')

# Crea la colonna 'geo_id' concatenando 'borough_id' e 'community_district'
slopes_df['geo_id'] = slopes_df['borough_id'].astype(str) + slopes_df['community_district'].astype(str)

# Converti 'geo_id' in intero (se necessario)
slopes_df['geo_id'] = slopes_df['geo_id'].astype(int)

# Salva il DataFrame aggiornato in un nuovo file CSV
slopes_df.to_csv('slopes_with_geo_id.csv', index=False)

print("Colonna 'geo_id' aggiunta e file salvato come 'slopes_with_geo_id.csv'.")