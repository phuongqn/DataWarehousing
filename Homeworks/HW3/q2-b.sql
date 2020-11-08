select Name, Continent, Population
from country
where Name like '%united%'
and Population >= 1000000;