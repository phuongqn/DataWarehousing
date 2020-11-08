Select name
From country
Where code not in (
Select CountryCode
From countrylanguage);