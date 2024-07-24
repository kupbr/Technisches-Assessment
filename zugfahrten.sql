select count(distinct z.Fahrtnummer) from zugfahrten z;

select f.Fahrtnummer, max(anz) from
(select z.Fahrtnummer , count(z.Fahrzeugnummer) anz 
from zugfahrten z group by z.Fahrtnummer
) f;

-- select Fahrtnummer, datepart(year, Abfahrtsdatum) from zugfahrten z 