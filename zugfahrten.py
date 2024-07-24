import pandas as pd
import numpy as np
import sqlite3 
import os
import logging

def log_invalid_rows(reason):
  null_mask = df_temp.isnull().any(axis=1)
  null_rows = df[null_mask]
  null_rows = null_rows.assign(grund=reason)
  if null_rows.shape[0] > 0:
    null_rows.to_csv('zugfahrten_errors.csv',mode='a', header=False)





logging.basicConfig(
    filename="zugfahrten.log",
    encoding="utf-8",
    filemode="a",
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M", 
    level=logging.INFO)

logging.info('Loading zugfahrten.csv')

os.chdir('C:/Users/morsx/Documents/docs-archive/Bewerbung 2024/ÖBB IT Lösungsentwickler Schwerpunkt Data Engineering/Technisches Assessment')
df = pd.read_csv('zugfahrten.csv')
logging.info('Rows found in file: %d', df.shape[0])
logging.info('Begin validation of file')

#Delete duplicates
logging.info('Looking for duplicate rows')
df_duplicates = df[df.duplicated(keep='first')]
df_duplicates.sort_values(by=['Fahrtnummer', 'Fahrzeugnummer'])
logging.info('Number of duplicates found: %d', df_duplicates.shape[0])
df_duplicates = df_duplicates.assign(grund='Duplicate')
df_duplicates.to_csv('zugfahrten_errors.csv',mode='a')

df.drop_duplicates(inplace=True)
logging.info('Remaining rows after removing duplicates: %d', df.shape[0])

# Removing rows with missing values
logging.info('Looking for rows with missing values')
null_mask = df.isnull().any(axis=1)
null_rows = df[null_mask]
null_rows = null_rows.assign(grund='Missing Values')
logging.info('Number of rows with missing values found: %d', null_rows.shape[0])

if null_rows.shape[0] >0:
    null_rows.to_csv('zugfahrten_errors.csv',mode='a', header=False)
# Deleting rows with empty cells
df = df.dropna()
logging.info('Remaining rows after removing missing values: %d', df.shape[0])
 
logging.info('Converting Columns to appropriate data types and filtering out illegal values')
logging.info('Abfahrtsdatum')
logging.info('Zeilen vor Umwandeln Abfahrtsdatum: %d', df.shape[0])
#df_temp is needed so that I can write the unchanged rows to the csv-log.
#df only has empty cells where a value could not be converted
df_temp = df
df_temp['Abfahrtsdatum'] = pd.to_datetime(df['Abfahrtsdatum'], format='mixed')
log_invalid_rows('Abfahrtsdatum could not be converted to datetime')
df = df_temp.dropna()
logging.info('Zeilen nach Umwandeln Abfahrtsdatum: %d', df.shape[0])

logging.info('Date 01.01.1900 will be treated as illegal date and removed')
illegal_date_rows = df[df["Abfahrtsdatum"] ==pd.to_datetime('1900-01-01')]
illegal_date_rows = illegal_date_rows.assign(grund='Abfahrtsdatum was 01.01.1900')
illegal_date_rows.to_csv('zugfahrten_errors.csv',mode='a', header=False)
df.drop(df.loc[df['Abfahrtsdatum']==pd.to_datetime('1900-01-01')].index, inplace=True)
logging.info('Rows after removing 01.01.1900: %d', df.shape[0])

logging.info('Abfahrtszeit')
logging.info('Zeilen vor Umwandeln Abfahrtszeit: %d', df.shape[0])
df_temp['Abfahrtszeit'] = pd.to_datetime(df['Abfahrtszeit'], format='mixed', errors='coerce')    
log_invalid_rows('Abfahrtszeit could not be converted to datetime')
df = df_temp.dropna()
logging.info('Zeilen nach Umwandeln Abfahrtszeit: %d', df.shape[0])

logging.info('Ankunftszeit')
logging.info('Zeilen vor Umwandeln Ankunftszeit: %d', df.shape[0])
df_temp['Ankunftszeit'] = pd.to_datetime(df['Ankunftszeit'], format='mixed', errors='coerce')
log_invalid_rows('Ankunftszeit could not be converted to datetime')
df = df_temp.dropna()
logging.info('Zeilen nach Umwandeln Abfahrtszeit: %d', df.shape[0])

logging.info('Zug id')
logging.info('Zeilen vor Umwandeln Zug id: %d', df.shape[0])
df_temp['Zug id'] = pd.to_numeric(df['Zug id'], errors='coerce')
log_invalid_rows('Zug id could not be converted to numeric')
df = df_temp.dropna()
logging.info('Zeilen nach Umwandeln Zug id: %d', df.shape[0])

logging.info('Buchungskreis')
logging.info('Zeilen vor Umwandeln Buchungskreis: %d', df.shape[0])
df_temp['Buchungskreis'] = pd.to_numeric(df['Buchungskreis'], errors='coerce')
log_invalid_rows('Buchungskreis could not be converted to numeric')
df = df_temp.dropna()
logging.info('Zeilen nach Umwandeln Buchungskreis: %d', df.shape[0])

logging.info('Entfernung km')
logging.info('Zeilen vor Umwandeln Entfernung km: %d', df.shape[0])
df_temp['Entfernung km'] = pd.to_numeric(df['Entfernung km'], errors='coerce')
log_invalid_rows('Entfernung km could not be converted to numeric')
df = df_temp.dropna()
logging.info('Zeilen nach Umwandeln Entfernung km: %d', df.shape[0])

logging.info('Passagierzahl')
logging.info('Zeilen vor Umwandeln Passagierzahl: %d', df.shape[0])
df_temp['Passagieranzahl'] = pd.to_numeric(df['Passagieranzahl'], errors='coerce')
log_invalid_rows('Passagierzahl could not be converted to numeric')
df = df_temp.dropna()
logging.info('Zeilen nach Umwandeln Passagierzahl: %d', df.shape[0])

logging.info('Abfahrtsbahnhof')
logging.info('Zeilen vor Umwandeln Abfahrtsbahnhof: %d', df.shape[0])
df_temp['Abfahrtsbahnhof'] = df['Abfahrtsbahnhof'].astype(str)
log_invalid_rows('Abfahrtsbahnhof could not be converted to string')
df = df_temp.dropna()
logging.info('Zeilen nach Umwandeln Abfahrtsbahnhof: %d', df.shape[0])

logging.info('Ankunftsbahnhof')
logging.info('Zeilen vor Umwandeln Ankunftsbahnhof: %d', df.shape[0])
df_temp['Ankunftsbahnhof'] = df['Ankunftsbahnhof'].astype(str)
log_invalid_rows('Ankunftsbahnhof could not be converted to string')
df = df_temp.dropna()
logging.info('Zeilen nach Umwandeln Ankunftsbahnhof: %d', df.shape[0])

logging.info('Starting calculations on data')

logging.info('Reisezeit in Minuten')
df['Reisezeit'] = (df['Ankunftszeit']-df['Abfahrtszeit'])
df['Reisezeit'] = df['Reisezeit'].dt.total_seconds()/60.0

logging.info('Adding Reisezeit in Stunden')
df['Reisezeit Stunden'] = df['Reisezeit'] / 60.0

logging.info('Adding Durchschnittsgeschwindigkeit')
df['Durchschnittsgeschwindigkeit'] = (df['Entfernung km']/df['Reisezeit Stunden'])

#TODO Gesamtzahl der Passagiere pro Zug pro Tag - Abfahrtsdatum
#gesamtzahl_df =  df.groupby(['Zug id', 'Abfahrtsdatum'])['Passagieranzahl'].sum()


# store to sqlite
conn = sqlite3.connect('zugfahrten.sqlite')
df.to_sql('zugfahrten', conn, if_exists='replace', index=False)



