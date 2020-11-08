Select c1.name
From country c1 left join country language c3
On c1.Code = c3.CountryCode
Where c3.Language is null;