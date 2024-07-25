##########################################################
### Technisches Assessment - Data Engineer / ETL
### Brigitte Kupka
### brigitte.kupka@gmx.at
##########################################################

import pandas as pd
import sqlite3 
import sys
import os

def log_invalid_rows(reason):
  null_mask = df_temp.isnull().any(axis=1)
  null_rows = df.loc[null_mask]
  null_rows = null_rows.assign(grund=reason)
  if null_rows.shape[0] > 0:
    null_rows.to_csv('zugfahrten_errors.csv',mode='a', header=False)


#####################################################
### 1 - EXTRACT #####################################
#####################################################
os.chdir(sys.path[0])
df = pd.read_csv('zugfahrten.csv')

#####################################################
### 2 - TRANSFORM ###################################
#####################################################
###
### Delete duplicates and write duplicate rows to csv log
###
df_duplicates = df[df.duplicated(keep='first')]
df_duplicates = df_duplicates.assign(grund='Duplicate')
df_duplicates.to_csv('zugfahrten_errors.csv',mode='a')
df.drop_duplicates(inplace=True)

###
### Remove rows with missing values 
###
# To be able to log the deleted rows with reason for deletion,
# a boolean mask is created to find the to be deleted rows in the dataframe, mark them and write them to csv log
# only after logging, the rows are dropped from the original dataframe

null_mask = df.isnull().any(axis=1)
null_rows = df[null_mask]
null_rows = null_rows.assign(grund='Missing Values')
if null_rows.shape[0] >0:
    null_rows.to_csv('zugfahrten_errors.csv',mode='a', header=False)
df = df.dropna()
 
###
### Convert datatypes in dataframe
###
#
# Attempting to convert Abfahrtsdatum to datetime
# df_temp is needed so that I can write the unchanged rows to the csv-log.
# since null values were already removed in the previous step, 
# df now only has empty cells where a value could not be converted
# mark the rows and save them to the csv log
df_temp = df
df_temp['Abfahrtsdatum'] = pd.to_datetime(df['Abfahrtsdatum'], format='mixed')
log_invalid_rows('Abfahrtsdatum could not be converted to datetime')
df = df_temp.dropna()

# Date 01.01.1900 will be treated as illegal date
# The rows are written to the csv log and removed from df
illegal_date_rows = df[df["Abfahrtsdatum"] ==pd.to_datetime('1900-01-01')]
illegal_date_rows = illegal_date_rows.assign(grund='Abfahrtsdatum was 01.01.1900')
illegal_date_rows.to_csv('zugfahrten_errors.csv',mode='a', header=False)
df.drop(df.loc[df['Abfahrtsdatum']==pd.to_datetime('1900-01-01')].index, inplace=True)

# angenommenes Datum des Exports 23.07.2024
# Abfahrtsdatum in der Zukunft wird entfernt
illegal_date_rows = df[df["Abfahrtsdatum"] >pd.to_datetime('2024-07-23')]
illegal_date_rows = illegal_date_rows.assign(grund='Abfahrtsdatum was in the future')
illegal_date_rows.to_csv('zugfahrten_errors.csv',mode='a', header=False)
df.drop(df.loc[df['Abfahrtsdatum']>pd.to_datetime('2024-07-23')].index, inplace=True)

# Versuche Abfahrtszeit in datetime umzuwandeln
# Anmerkung: Abfahrtszeit enthält ein Datum, das vom Abfahrtsdatum stark abweicht
# Ich nehme an, das ist Prozessbedingt und nicht relevant
# Umwandeln in datetime, ungültige Zeilen loggen, leere Zeilen löschen, wo die Konvertierung fehlgeschlagen ist
df_temp['Abfahrtszeit'] = pd.to_datetime(df['Abfahrtszeit'], format='mixed', errors='coerce')    
log_invalid_rows('Abfahrtszeit could not be converted to datetime')
df = df_temp.dropna()
# Abfahrtszeit in der Zukunft wird entfernt
# Obwohl das Datum in Abfahrtszeit und Ankunftszeit eigentlich nicht verwendet ist, gibt es
# zumindest einen fehlerhaften Eintrag mit Datum in der Zukunft, der eine falsche Reisezeit ergibt
# Wird daher entfernt
illegal_date_rows = df[df["Abfahrtszeit"] >pd.to_datetime('2024-07-23')]
illegal_date_rows = illegal_date_rows.assign(grund='Abfahrtszeit was in the future')
illegal_date_rows.to_csv('zugfahrten_errors.csv',mode='a', header=False)
df.drop(df.loc[df['Abfahrtszeit']>pd.to_datetime('2024-07-23')].index, inplace=True)

# Versuche Abfahrtszeit in datetime umzuwandeln - same as above
df_temp['Ankunftszeit'] = pd.to_datetime(df['Ankunftszeit'], format='mixed', errors='coerce')
log_invalid_rows('Ankunftszeit could not be converted to datetime')
df = df_temp.dropna()

# Entferne Abfahrstzeit in der Zukunft - siehe Anmerkung oben
illegal_date_rows = df[df["Ankunftszeit"] >pd.to_datetime('2024-07-23')]
illegal_date_rows = illegal_date_rows.assign(grund='Ankunftszeit was in the future')
illegal_date_rows.to_csv('zugfahrten_errors.csv',mode='a', header=False)
df.drop(df.loc[df['Ankunftszeit']>pd.to_datetime('2024-07-23')].index, inplace=True)


# Versuche Zug id in numeric umzuwandeln
# Ungültige Zeilen werden in CSV geschrieben und anschließend gelöscht
df_temp['Zug id'] = pd.to_numeric(df['Zug id'], errors='coerce')
log_invalid_rows('Zug id could not be converted to numeric')
df = df_temp.dropna()


# Versuche Buchungskreis in numeric umzuwandeln
df_temp['Buchungskreis'] = pd.to_numeric(df['Buchungskreis'], errors='coerce')
log_invalid_rows('Buchungskreis could not be converted to numeric')
df = df_temp.dropna()

# Versuche Entfernung km in numeric umzuwandeln
df_temp['Entfernung km'] = pd.to_numeric(df['Entfernung km'], errors='coerce')
log_invalid_rows('Entfernung km could not be converted to numeric')
df = df_temp.dropna()

# Versuche Passagieranzahl in numeric umzuwandeln
df_temp['Passagieranzahl'] = pd.to_numeric(df['Passagieranzahl'], errors='coerce')
log_invalid_rows('Passagierzahl could not be converted to numeric')
df = df_temp.dropna()

# Versuche Abfahrtsbahnhof in String umzuwandeln
df_temp['Abfahrtsbahnhof'] = df['Abfahrtsbahnhof'].astype(str)
log_invalid_rows('Abfahrtsbahnhof could not be converted to string')
df = df_temp.dropna()

# Versuche Ankunftsbahnhof in String umzuwandeln
df_temp['Ankunftsbahnhof'] = df['Ankunftsbahnhof'].astype(str)
log_invalid_rows('Ankunftsbahnhof could not be converted to string')
df = df_temp.dropna()

###
### Berechnen zusätzlicher Information
###
# Reisezeit in Minuten
df['Reisezeit'] = (df['Ankunftszeit']-df['Abfahrtszeit'])
df['Reisezeit'] = df['Reisezeit'].dt.total_seconds()/60.0

# Reisezeit in Stunden
df['Reisezeit Stunden'] = df['Reisezeit'] / 60.0

# Durchschnittsgeschwindigkeit
# Einige Zeilen haben eine unrealitische Durchschnittsgeschwindigkeit, wegen Kombinationen von kurzen Reisezeiten für große Distanz
# Klären und ggf. entfernen
df['Durchschnittsgeschwindigkeit'] = (df['Entfernung km']/df['Reisezeit Stunden'])


# Gesamtzahl der Passagiere pro Zug pro Tag - Abfahrtsdatum
# Achtung: Abfahrtsdatum ist nicht sauber, es gibt zwischendurch anscheinend fehlerhafte Datumseinträge bei einzelnen Zeilen
# Man sieht das gut, wenn man die Groupby Reihenfolge umgekehrt und zuerst nach Zug Id und dann nach Abfahrtsdatum Gruppiert
# eine anderer Ansatz findet sich in zugfahrten.sql, aber eine optimale Lösung ist schwierig. Welches Datum nimmt man?
# Das das pro Fahrt am häufigsten vorkommt? 
gesamtzahl_df =  df.groupby(['Abfahrtsdatum','Zug id',]).Passagieranzahl.agg(['sum'])
#print(gesamtzahl_df.to_string())

#####################################################
### 3 - LOAD ########################################
#####################################################
# SLQ Lite Tabelle aus den Daten erstellen
conn = sqlite3.connect('zugfahrten.sqlite')
df.to_sql('zugfahrten', conn, if_exists='replace', index=False)

# Dataframe nach Transformation auch für weitere Betrachtungen in neues csv schreiben
df.to_csv('zugfahrten_transformed.csv', index=False)



