##########################################################
### Technisches Assessment - Data Engineer / ETL
### Brigitte Kupka
### brigitte.kupka@gmx.at
##########################################################
import sys
import os
import pandas as pd
import seaborn as sns
import networkx as nx
import difflib
import matplotlib.pyplot as plt

def printBoxPlot(spalte):
    fig1, ax1 = plt.subplots()
    ax1.set_title(spalte)
    ax1.boxplot(df[spalte])
    plt.show()

## Transformierte Daten einlesen

os.chdir(sys.path[0])
df = pd.read_csv('zugfahrten_transformed.csv')

#########################################################
### Ausreißeranalyse und sonstige Betrachtungen der Daten

# Boxplots geben einen guten Eindruck, ob die Daten nach der erfolgten Transformation valide sind
# oder zusätzliche Bereinigungen notwendig sind

printBoxPlot("Messung")
# --> die meisten Werte der Messung sind positiv zwischen 1000 und 5000. 
# Es gibt eine Anzahl Werte in diesem Bereich aber mit negativem Vorzeichen
# Inhaltliche Klärung, wie diese Werte entstanden sind bzw. ob sie gültig sind, oder ob es sich um Messfehler handelt ist notwendig

printBoxPlot("Reisezeit")
# --> es gibt keine Ausreißer, nachdem die Abfahrts- und Ankunftszeiten in der Zukunft gelöscht wurden
printBoxPlot("Passagieranzahl")
# --> es gibt keine Ausreißer bei der Passagieranzahl
printBoxPlot("Entfernung km")
# --> es gibt keine Ausreißer bei der Entfernung
printBoxPlot("Durchschnittsgeschwindigkeit")
# --> es gibt einige Ausreißer, bedingt durch Fehler bei Entfernung oder Reisezeit, die erst in der Darstellung als 
#       Geschwindigkeit sichtbar werden, wenn die beiden Daten nicht plausibel zueinanderpassen, aber einzeln betrachtet 
#       plausibel erscheinen

# Ankunftsbanhnhof und Abfahrtsbahnhof sehen auf den ersten Blick sehr sauber aus 
# Gibt es eventuell doch Tippfehler, wo die Bahnhöfe zusammengefasst werden könnten?
# difflib.get_close_matches kann Kandidaten für eine gründlichere Bereinigung ausgeben
# Bsp: Williamshire: Williamshire, Williamsshire
#      West Sarah: West Sarah, West Sara
#      Kristineshire: Kristineshire, Kristinshire
#       .... 

bahnhoefe = df[["Abfahrtsbahnhof"]]
bahnhoefe.drop_duplicates(inplace=True)
bahnhoefe.sort_values(by=['Abfahrtsbahnhof'], inplace=True)
#bahnhoefe = bahnhoefe.head(1000)
for index, row in bahnhoefe.iterrows():
    close_matches = difflib.get_close_matches(row["Abfahrtsbahnhof"], bahnhoefe["Abfahrtsbahnhof"], 5, 0.9)
    # falls mehr als ein (eines ist immer das identische, da ich jeden Wert mit der kompletten Liste vergleiche) 
    # match gefunden wurde, kann man sich die kandidaten ansehen. 
    #if len(close_matches) > 1:
    #   print(row["Abfahrtsbahnhof"] + ': ' + ' '.join([str(elem) for elem in close_matches]))
   

### Zum Spaß probiert: Bahnhöfe und Distanz als Netzwerk plotten ;)
def printNetwork():
   bahnhoefe = df[["Abfahrtsbahnhof", "Ankunftsbahnhof", "Entfernung km"]]
   bahnhoefe.drop_duplicates(inplace=True)
   bahnhoefe.sort_values(by=['Ankunftsbahnhof'], inplace=True)
   bahnhoefe = bahnhoefe.head(10)
   G = nx.Graph()
   for index, row in bahnhoefe.iterrows():
      G.add_edge(row["Abfahrtsbahnhof"], row["Ankunftsbahnhof"], weight=row["Entfernung km"])
   pos = nx.kamada_kawai_layout(G, weight='weight')
   #pos = nx.spring_layout(G, weight='weight')
   nx.draw(G,pos)
   labels = {k: round(v,2) for k,v in nx.get_edge_attributes(G,'weight').items()}
   nx.draw_networkx_edge_labels(G,pos,edge_labels=labels)
   nx.draw_networkx_labels(G,pos)
   plt.show()