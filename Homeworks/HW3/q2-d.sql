Select Name 
From country, countrylanguage
Where Code=CountryCode
Group by CountryCode
Having sum(IsOfficial = 'F')>=10
Order by sum(IsOfficial = 'F') desc;