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
### Lösche Duplikate und schreibe die betroffenen Zeilen in das CSV Log
###
df_duplicates = df[df.duplicated(keep='first')]
df_duplicates = df_duplicates.assign(grund='Duplicate')
df_duplicates.to_csv('zugfahrten_errors.csv',mode='a')
df.drop_duplicates(inplace=True)

###
### Entferne Zeilen mit fehlenden Werten
###
# Damit die gelöschten Zeilen mit dem Grund der Löschung ins Log geschrieben werden können,
# wird eine bool Maske erstellt, die es ermöglicht die betroffenen Zeilen im Dataframe zu finden,
# sie zu markieren und sie in das CSV Log zu schreiben.
# Erst nachdem sie so geloggt wurden, werden sie aus dem eigentlichen dataframe gelöscht.

null_mask = df.isnull().any(axis=1)
null_rows = df[null_mask]
null_rows = null_rows.assign(grund='Missing Values')
if null_rows.shape[0] >0:
    null_rows.to_csv('zugfahrten_errors.csv',mode='a', header=False)
df = df.dropna()
 
###
### Datentypen im Dataframe umwandeln
###
#
# Versuche Abfahrtsdatum in datetime umzuwandeln - bei Werten die fehlschlagen, wird der Wert gelöscht.
# df_temp wird dafür verwendet, die unveränderten Zeilen mit illegalen Werten in das CSV Log schreiben zu können
# Nachdem die Werte, die schon zu beginn gefehlt haben schon im vorigen Schritt aus df entfernt wurden, 
# sind die leeren Zellen in df jetzt genau die, wo der Wert nicht auf den gewünschten Datentyp geändert werden konnte.
# Die Zeilen werden markiert und in das CSV Fehlerlog geschrieben.
# Diese Schritte wiederholen sich für alle Variablen/Typumwandlungen einzeln, damit der Grund der Löschung im Log jeweils
# informativ ist.

df_temp = df
df_temp['Abfahrtsdatum'] = pd.to_datetime(df['Abfahrtsdatum'], format='mixed')
log_invalid_rows('Abfahrtsdatum could not be converted to datetime')
df = df_temp.dropna()

# Das Datum 01.01.1900 wird als ungültiges Datum behandelt
# Die Zeilen werden in das CSV Log geschrieben und aus dem dataframe entfernt
illegal_date_rows = df[df["Abfahrtsdatum"] ==pd.to_datetime('1900-01-01')]
illegal_date_rows = illegal_date_rows.assign(grund='Abfahrtsdatum was 01.01.1900')
illegal_date_rows.to_csv('zugfahrten_errors.csv',mode='a', header=False)
df.drop(df.loc[df['Abfahrtsdatum']==pd.to_datetime('1900-01-01')].index, inplace=True)

# Annahme: Datum des Exports 23.07.2024 - wenn man davon ausgeht, so gibt es ungültige Abfahrtsdaten in der Zukunft
# Abfahrtsdatum in der Zukunft wird entfernt
illegal_date_rows = df[df["Abfahrtsdatum"] >pd.to_datetime('2024-07-23')]
illegal_date_rows = illegal_date_rows.assign(grund='Abfahrtsdatum was in the future')
illegal_date_rows.to_csv('zugfahrten_errors.csv',mode='a', header=False)
df.drop(df.loc[df['Abfahrtsdatum']>pd.to_datetime('2024-07-23')].index, inplace=True)

# Versuche Abfahrtszeit in datetime umzuwandeln
# Anmerkung: Abfahrtszeit enthält ein Datum, das vom Abfahrtsdatum stark abweicht
# Ich nehme an, das ist Prozessbedingt und nicht relevant, bzw. verwende das Datum nur für die Berechnung der Reisedauer
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