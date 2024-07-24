-- Wieviele Zugfahrten gibt es insgesamt?
select count(distinct z.Fahrtnummer) from zugfahrten z;

-- Welche Zugfahrt hat die meisten Fahrzeugnummern?
-- Edit: Nach Bereinigung, manche Zeilen wurden wegen fehlender Werte in anderen Spalten entfernt

select f.Fahrtnummer, max(anz) from
(select z.Fahrtnummer , count(z.Fahrzeugnummer) anz 
from zugfahrten z group by z.Fahrtnummer
) f;

-- Wieviele Zugfahrten wurden in den jeweiligen Jahren durchgeführt
-- Achtung, das geht davon aus, dass das Abfahrtsdatum bei jeder Zeile für eine Fahrtnummer gleich ist
-- Wenn es Fehler beim Jahr gibt, dann stimmt das hier nicht
select count(distinct z.fahrtnummer), substr(z.Abfahrtsdatum,1,4) jahr
from zugfahrten z 
group by jahr order by jahr

-- Frühestes Abfahrtsdatum und Durchschnitt der Messung der Zugfahrten der jeweiligen Buchungskreise
-- Frühestes Datum als Text? max und min funktioniert auch auf den string
-- TODO Durchschnitt pro Fahrt und dann pro Buchungskreis oder Durchschnitt über alle Fahrten pro Buchungskreis?
select z.Buchungskreis , min(z.abfahrtsdatum), avg(z.messung) 
from zugfahrten z 
group by z.Buchungskreis 


-- Welcher Buchungskreis hat die höchste Gesamtsumme bei den Zugfahrten in den jeweiligen Jahren?
SELECT max(anz.anz_fahrten), anz.Buchungskreis from 
(select count(distinct z.fahrtnummer) anz_fahrten, z.Buchungskreis 
from zugfahrten z 
group by z.Buchungskreis ) anz

-- Bei den verschiedenen Buchungskreisen wird von der aktuellsten Zugfahrt die Fahrzeugnummer mi tder zweithöchsten Messung gesucht
select Fahrtnummer , Buchungskreis , max(Abfahrtszeit) from zugfahrten z
group by Buchungskreis 

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



---- Checks 
-- Vergleiche Abfahrtsdatum mit dem Datum in der Süalte Abfahrtszeit
select Fahrtnummer, Fahrzeugnummer, SUBSTR(Abfahrtsdatum,1,10) datum, SUBSTR(Abfahrtszeit,1,10) zeit from zugfahrten z
where SUBSTR(Abfahrtsdatum,1,10) <> SUBSTR(Abfahrtszeit,1,10)   

-- Ist zug id immer gleich fahrtnummer?
select * from zugfahrten z 
where z."Zug id" == z.Fahrtnummer 