from pyspark import SparkContext

sc = SparkContext(master = 'local[*]', appName = 'no-lang')

country_language = set(sc.textFile('countrylanguage.csv').map(lambda x: x.split(',')).\
                       map(lambda x: (x[0].strip("' '"))).collect())

country = sc.textFile('country.csv').map(lambda x: x.split(',')).map(lambda x: (x[0].strip("' "), x[1].strip("' '")))

no_lang = country.filter(lambda x: x[0] not in country_language).collect()

for n in no_lang:
    print(n[1])
