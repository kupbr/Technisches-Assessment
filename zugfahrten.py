import pandas as pd
import numpy as np
import sqlite3 
import os
import logging



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
# TODO Numbers don't add up
logging.info('Looking for duplicate rows')
df_duplicates = df[df.duplicated(keep=False)]
df_duplicates.sort_values(by=['Fahrtnummer', 'Fahrzeugnummer'])
logging.info('Number of duplicates found: %d', df_duplicates.shape[0])
#logging.info(df_duplicates.to_string())
df.drop_duplicates(inplace=True)
logging.info('Remaining rows after removing duplicates: %d', df.shape[0])

# Removing rows with missing values
logging.info('Looking for rows with missing values')
null_mask = df.isnull().any(axis=1)
null_rows = df[null_mask]
logging.info('Number of rows with missing values found: %d', null_rows.shape[0])
#logging.info("The following rows contain missing values and will be removed")
if null_rows.shape[0] >0:
    logging.info(null_rows.to_string())
# Deleting rows with empty cells
df = df.dropna()
logging.info('Remaining rows after removing missing values: %d', df.shape[0])
 
logging.info('Converting Columns to appropriate data types and filtering out illegal values')
logging.info('Abfahrtsdatum')
# Convert different Date formats to YYYY-MM-DD - SettingWithCopyWarning
logging.info('Zeilen vor Umwandeln Abfahrtsdatum: %d', df.shape[0])
df['Abfahrtsdatum'] = pd.to_datetime(df['Abfahrtsdatum'], format='mixed')
logging.info('Zeilen nach Umwandeln Abfahrtsdatum: %d', df.shape[0])
null_mask = df.isnull().any(axis=1)
null_rows = df[null_mask]
logging.info('Number of rows with invalid Abfahrtsatum found: %d', null_rows.shape[0])
if null_rows.shape[0] > 0:
    logging.info(null_rows.to_string())
df = df.dropna()
logging.info('Date 01.01.1900 will be treated as illegal date and removed')
# TODO Log Rows with 1900-01-01

df.drop(df.loc[df['Abfahrtsdatum']==pd.to_datetime('1900-01-01')].index, inplace=True)
logging.info('Rows after removing 01.01.1900: %d', df.shape[0])

logging.info('Abfahrtszeit')
logging.info('Zeilen vor Umwandeln Abfahrtszeit: %d', df.shape[0])
df['Abfahrtszeit'] = pd.to_datetime(df['Abfahrtszeit'], format='mixed', errors='coerce')    
logging.info('Zeilen nach Umwandeln Abfahrtszeit: %d', df.shape[0])
null_mask = df.isnull().any(axis=1)
null_rows = df[null_mask]
logging.info('Number of rows with invalid Abfahrtszeit found: %d', null_rows.shape[0])
if null_rows.shape[0] > 0:
    logging.info(null_rows.to_string())
df = df.dropna()

logging.info('Ankunftszeit')
logging.info('Zeilen vor Umwandeln Ankunftszeit: %d', df.shape[0])
df['Ankunftszeit'] = pd.to_datetime(df['Ankunftszeit'], format='mixed', errors='coerce')
logging.info('Zeilen nach Umwandeln Abfahrtszeit: %d', df.shape[0])
null_mask = df.isnull().any(axis=1)
null_rows = df[null_mask]
logging.info('Number of rows with invalid Ankunftszeit found: %d', null_rows.shape[0])
if null_rows.shape[0] > 0:
    logging.info(null_rows.to_string())
df = df.dropna()

logging.info('Zug id')
logging.info('Zeilen vor Umwandeln Zug id: %d', df.shape[0])
df['Zug id'] = pd.to_numeric(df['Zug id'], errors='coerce')
logging.info('Zeilen nach Umwandeln Zug id: %d', df.shape[0])
null_mask = df.isnull().any(axis=1)
null_rows = df[null_mask]
logging.info('Number of rows with invalid Zug id found: %d', null_rows.shape[0])
if null_rows.shape[0] > 0:
    logging.info(null_rows.to_string())
df = df.dropna()

logging.info('Zug id')
logging.info('Zeilen vor Umwandeln Zug id: %d', df.shape[0])
df['Buchungskreis'] = pd.to_numeric(df['Buchungskreis'], errors='coerce')
logging.info('Zeilen nach Umwandeln Zug id: %d', df.shape[0])
null_mask = df.isnull().any(axis=1)
null_rows = df[null_mask]
logging.info('Number of rows with invalid Zug id found: %d', null_rows.shape[0])
if null_rows.shape[0] > 0:
    logging.info(null_rows.to_string())
df = df.dropna()

logging.info('Entfernung km')
logging.info('Zeilen vor Umwandeln Entfernung km: %d', df.shape[0])
df['Entfernung km'] = pd.to_numeric(df['Entfernung km'], errors='coerce')
logging.info('Zeilen nach Umwandeln Entfernung km: %d', df.shape[0])
null_mask = df.isnull().any(axis=1)
null_rows = df[null_mask]
logging.info('Number of rows with invalid Entfernung km found: %d', null_rows.shape[0])
if null_rows.shape[0] > 0:
    logging.info(null_rows.to_string())
df = df.dropna()

logging.info('Passagierzahl')
logging.info('Zeilen vor Umwandeln Passagierzahl: %d', df.shape[0])
df['Passagieranzahl'] = pd.to_numeric(df['Passagieranzahl'], errors='coerce')
logging.info('Zeilen nach Umwandeln Passagierzahl: %d', df.shape[0])
null_mask = df.isnull().any(axis=1)
null_rows = df[null_mask]
logging.info('Number of rows with invalid Passagierzahl found: %d', null_rows.shape[0])
if null_rows.shape[0] > 0:
    logging.info(null_rows.to_string())
df = df.dropna()

logging.info('Abfahrtsbahnhof')
logging.info('Zeilen vor Umwandeln Abfahrtsbahnhof: %d', df.shape[0])
df['Abfahrtsbahnhof'] = df['Abfahrtsbahnhof'].astype(str)
logging.info('Zeilen nach Umwandeln Abfahrtsbahnhof: %d', df.shape[0])
null_mask = df.isnull().any(axis=1)
null_rows = df[null_mask]
logging.info('Number of rows with invalid Abfahrtsbahnhof found: %d', null_rows.shape[0])
if null_rows.shape[0] > 0:
    logging.info(null_rows.to_string())
df = df.dropna()

logging.info('Ankunftsbahnhof')
logging.info('Zeilen vor Umwandeln Ankunftsbahnhof: %d', df.shape[0])
df['Ankunftsbahnhof'] = df['Ankunftsbahnhof'].astype(str)
logging.info('Zeilen nach Umwandeln Ankunftsbahnhof: %d', df.shape[0])
null_mask = df.isnull().any(axis=1)
null_rows = df[null_mask]
logging.info('Number of rows with invalid Ankunftsbahnhof found: %d', null_rows.shape[0])
if null_rows.shape[0] > 0:
    logging.info(null_rows.to_string())
df = df.dropna()

logging.info('Starting calculations on data')

logging.info('Reisezeit in Minuten')
df['Reisezeit'] = (df['Ankunftszeit']-df['Abfahrtszeit'])
df['Reisezeit'] = df['Reisezeit'].dt.total_seconds()/60.0

logging.info('Adding Reisezeit in Stunden')
df['Reisezeit Stunden'] = df['Reisezeit'] / 60.0

logging.info('Adding Durchschnittsgeschwindigkeit')
df['Durchschnittsgeschwindigkeit'] = (df['Entfernung km']/df['Reisezeit Stunden'])

#TODO Gesamtzahl der Passagiere pro Zug pro Tag - Abfahrtsdatum? Abfahrtszeit? Nachfragen
#gesamtzahl_df =  df.groupby(['Zug id', 'Abfahrtsdatum'])['Passagieranzahl'].sum()


# store to sqlite
conn = sqlite3.connect('zugfahrten.sqlite')
df.to_sql('zugfahrten', conn, if_exists='replace', index=False)



