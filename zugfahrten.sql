------------------------------------------------------------
--- Technisches Assessment - Data Engineer / ETL
--- Brigitte Kupka
--- brigitte.kupka@gmx.at
------------------------------------------------------------

------------------------------------------------------------
-- Wieviele Zugfahrten gibt es insgesamt?
select count(distinct z.Fahrtnummer) from zugfahrten z;

------------------------------------------------------------
-- Welche Zugfahrt hat die meisten Fahrzeugnummern?
-- Anmerkung: Nach Bereinigung, denn manche Zeilen wurden wegen fehlender Werte in anderen Spalten entfernt

select f.Fahrtnummer, max(anz) from
(select z.Fahrtnummer , count(z.Fahrzeugnummer) anz 
from zugfahrten z group by z.Fahrtnummer
) f;

------------------------------------------------------------
-- Wieviele Zugfahrten wurden in den jeweiligen Jahren durchgeführt?
-- Achtung, das geht davon aus, dass das Abfahrtsdatum bei jeder Zeile für eine Fahrtnummer gleich ist
-- Es gibt aber Zeilen, wo anscheinend fehlerhafte Abfahrsdatum bei einzelnen Fahrzeugen vorkommen. Diese sind hier getrennt gezählt
select count(distinct z.fahrtnummer), substr(z.Abfahrtsdatum,1,4) jahr
from zugfahrten z 
group by jahr order by jahr

-- Etwas bessere Möglichkeit - nehme immer nur das Abfahrtsdatum vom ersten Fahrzeug, statt distinct auf fahrtnummer wie oben, dadurch fallen
-- falsche Datumseinträge bei den Zeilen weg
-- Einige sind trotzdem noch falsch, wenn das fehlerhafte Datum beim ersten Fahrzeug steht
-- Eigentlich gehören diese Fahrten raus. 
select count(fahrten.Fahrtnummer), fahrten.jahr
from
	(select z.fahrtnummer, substr(z.Abfahrtsdatum,1,4) jahr
	from zugfahrten z 
	group by z.Fahrtnummer 
	having min(z.Fahrzeugnummer)) fahrten
group by jahr
order by jahr


------------------------------------------------------------
-- Frühestes Abfahrtsdatum und Durchschnitt der Messung der Zugfahrten der jeweiligen Buchungskreise
-- Frühestes Datum als Text - sqlite. max und min funktioniert auch auf den string
select z.Buchungskreis , min(z.abfahrtsdatum), avg(z.messung) 
from zugfahrten z 
group by z.Buchungskreis 

------------------------------------------------------------
-- Welcher Buchungskreis hat die höchste Gesamtsumme bei den Zugfahrten in den jeweiligen Jahren?
SELECT max(anz.anz_fahrten), anz.Buchungskreis from 
(select count(distinct z.fahrtnummer) anz_fahrten, z.Buchungskreis 
from zugfahrten z 
group by z.Buchungskreis ) anz


------------------------------------------------------------
-- Bei den verschiedenen Buchungskreisen wird von der aktuellsten Zugfahrt die Fahrzeugnummer mit der zweithöchsten Messung gesucht 
-- Ranks zu Messungen 
select m_ranks.Buchungskreis, m_ranks.Fahrzeugnummer
from (
select z.Buchungskreis , z.Abfahrtsdatum, z.Fahrtnummer, z.Fahrzeugnummer, z.Messung, 
		rank() over (partition by z.Fahrtnummer order by Messung desc) as messung_rank 
from zugfahrten z 
order by z.Buchungskreis, z.Fahrtnummer,  messung_rank  asc) m_ranks
where m_ranks.messung_rank = 2
group by m_ranks.Buchungskreis
having max(m_ranks.Abfahrtsdatum)


------------------------------------------------------------
---- Diverse Checks 
-- Vergleiche Abfahrtsdatum mit dem Datum in der Spalte Abfahrtszeit
select Fahrtnummer, Fahrzeugnummer, SUBSTR(Abfahrtsdatum,1,10) datum, SUBSTR(Abfahrtszeit,1,10) zeit from zugfahrten z
where SUBSTR(Abfahrtsdatum,1,10) <> SUBSTR(Abfahrtszeit,1,10)   

-- Ist zug id immer gleich fahrtnummer?
select * from zugfahrten z 
where z."Zug id" == z.Fahrtnummer 

-- range der messungen - siehe zugfahrten_add_code.py für boxplot
select min(messung), max(messung), avg(messung), max(messung) - min(messung) range
from zugfahrten z;

-- "min(messung)"	"max(messung)"	"avg(messung)"		"range"
-- -4980.23			4999.91			2735.184631826786	9980.14

-- range abfahrtsdatum
-- hier liegt auch nahe, dass es einige fehlerhafte Daten gibt, da die Datumswerte innerhalb einer Zugfahrt bei den verschiedenen
-- Fahrzeugen nicht einheitlich sind. Das zu korrigieren ist ohne weiteres Wissen schwierig, da man rein aufgrund der Häufigkeit eines Datums
-- ableiten könnte welches das Richtige ist. (wenn bei 9 Fahrzeugen ein Datum steht und bei 1 ein anderes, dann ist es zwar naheliegend,
-- dass das eine Datum falsch ist, aber ich würde es inhaltlich noch klären wollen
select min(Abfahrtsdatum), max(Abfahrtsdatum)
from zugfahrten z 

-- visueller check, wie die Qualität der Bahnhofsnamen aussieht
select distinct Abfahrtsbahnhof 
from zugfahrten z 
union
select distinct Ankunftsbahnhof
from zugfahrten z2 
order by 1

-- Check Durchschnittsgeschwindigkeit
-- minmax
-- Manche Züge sind auffallend schnell -- da passt etwas bei Entfernung oder Geschwindigkeit nicht

select distinct z."Zug id" , z.Durchschnittsgeschwindigkeit, z.Abfahrtszeit, z.Ankunftszeit, z."Entfernung km" 
from zugfahrten z 
where z.Durchschnittsgeschwindigkeit  > 200
order by z.Durchschnittsgeschwindigkeit  desc

-- 984 Züge über 200km/h im Durchschnitt sind eine Menge - wie einheitlich ist die Distanz zwischen den gleichen Bahnhöfen
-- für welche Bahnhöfe gibt es unterschiedilche Distanzen? -- Beide Richtungen werden ignoriert
select count(*), z.Abfahrtsbahnhof , z.Ankunftsbahnhof , z."Entfernung km" from zugfahrten z 
group by z.Abfahrtsbahnhof , z.Ankunftsbahnhof , z."Entfernung km" 
order by 2,3