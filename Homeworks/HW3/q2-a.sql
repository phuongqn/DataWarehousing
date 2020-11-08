select c1.name , c1.Population
from city c1 join country c2 
on c1.countryCode = c2.Code 
where c2.name = 'United States' 
order by c1.Population desc limit 10;