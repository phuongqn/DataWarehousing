select Language
from countrylanguage as c1
left join country c2
on c1.CountryCode = c2.Code
where c1.IsOfficial = 'T'
and Population > 1000000
group by Language
order by sum(Population) desc
limit 10;