import sys
import csv

if __name__ == "__main__":
    
    country=sys.argv[1]
    countrylanguage=sys.argv[2]
    
    countries = csv.DictReader(open(country))
    languages = csv.DictReader(open(countrylanguage))
    
#iterarte through country table O(m) time, O(m) space (through m rows)
    c_dict = {} #{code:pop}
    for row in countries:
        code = row['Code']
        pop= int(row['Population'])
        if pop > 1000000.0:
            if code not in c_dict:
                c_dict[code]=0
            c_dict[code] = pop
            
#iterarte through countrylanguae table O(n) time, O(n) space (through n rows)
    l_dict = {} #{language:pop}
    for row in languages:
        cc= row['CountryCode']
        i_o = row['IsOfficial']
        if (cc not in c_dict) or (i_o == 'F'):
            continue

        pop = c_dict[cc]
        language = row['Language']
        if language not in l_dict:
            l_dict[language]=0
        l_dict[language] += pop
        
#iterarte through l_dict O(n) time, O(n) space (through n rows)
    tuple_list= [(k, v) for k, v in l_dict.items()]

#sort through list above O(n log n)
    sorted_list = sorted(tuple_list, reverse = True, key=lambda item: item[1])
    top10 = sorted_list[0:10]
    top10 = [x[0] for x in top10]
    for country in top10:
        print(country)

