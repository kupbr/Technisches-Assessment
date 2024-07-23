import pandas as pd
import numpy as np
import sqlite3 
from pandas._libs.tslibs.parsing import DateParseError
import os
current_directory = os.getcwd()
print(current_directory) 	
os.chdir('C:/Users/morsx/Documents/docs-archive/Bewerbung 2024/ÖBB IT Lösungsentwickler Schwerpunkt Data Engineering/Technisches Assessment')
df = pd.read_csv('zugfahrten.csv', infer_datetime_format = True)
# Deleting rows with empty cells
new_df = df.dropna()
# Row count dropped from 26250 to 25755
# Which rows were dropped? --> Log
only_na = df[~df.index.isin(new_df.index)]

new_df.info()

# Convert different Date formats to YYYY-MM-DD - SettingWithCopyWarning
new_df['Abfahrtsdatum'] = pd.to_datetime(new_df['Abfahrtsdatum'], format='mixed')
# TODO hier muss ich vorher die falschen Zeilen finden, damit ich sie loggen kann
new_df['Abfahrtszeit'] = pd.to_datetime(new_df['Abfahrtszeit'], format='mixed', errors='coerce')    
# TODO hier muss ich vorher die falschen Zeilen finden, damit ich sie loggen kann
new_df['Ankunftszeit'] = pd.to_datetime(new_df['Ankunftszeit'], format='mixed', errors='coerce')
# TODO hier muss ich vorher die falschen Zeilen finden, damit ich sie loggen kann
new_df['Zug id'] = pd.to_numeric(new_df['Zug id'], errors='coerce')
new_df['Buchungskreis'] = pd.to_numeric(new_df['Buchungskreis'], errors='coerce')
new_df['Entfernung km'] = pd.to_numeric(new_df['Entfernung km'], errors='coerce')
new_df['Passagieranzahl'] = pd.to_numeric(new_df['Passagieranzahl'], errors='coerce')
new_df['Abfahrtsbahnhof'] = new_df['Abfahrtsbahnhof'].astype(str)
new_df['Ankunftsbahnhof'] = new_df['Ankunftsbahnhof'].astype(str)
new_df = new_df.dropna()

# 22709 rows left 

# Check Zug id for valid values
# so kriegt man eine Liste der Werte die nicht numerisch sind
# where values cant be coerced, they are treated as nulls.
# non_numeric_zug_id = new_df[pd.to_numeric(new_df['Zug id'], errors='coerce').isnull()]['Zug id'].unique()

new_df.info()

#TODO in Minuten!
new_df['Reisezeit'] = (new_df['Ankunftszeit']-new_df['Abfahrtszeit']).astype('timedelta64[s]')
print(new_df['Reisezeit'])
new_df['Reisezeit Stunden'] = new_df['Reisezeit'] / np.timedelta64(1, 'h')
print(new_df['Reisezeit Stunden']) 
print(new_df['Entfernung km'])
new_df['Durchschnittsgeschwindigkeit'] = (new_df['Entfernung km']/new_df['Reisezeit Stunden'])
print(new_df['Durchschnittsgeschwindigkeit'])

#TODO die Struktur hat kein richtiges Format
gesamtzahl_df =  new_df.groupby(['Zug id', 'Abfahrtsdatum'])['Passagieranzahl'].sum()
gesamtzahl_df.info()
print(gesamtzahl_df)

# store to sqlite
conn = sqlite3.connect('zugfahrten.sqlite')
new_df.to_sql('zugfahrten', conn, if_exists='replace', index=False)
bla = pd.read_sql('select * from zugfahrten', conn)