from pyspark import SparkContext

sc = SparkContext(master = 'local[*]', appName = 'unofficial10')

country_lang = sc.textFile('countrylanguage.csv').map(lambda x: x.split(',')).\
    map(lambda x: tuple([s.strip("' ") for s in x]))
country = sc.textFile('country.csv').map(lambda x: x.split(',')).map(lambda x: (x[0].strip("' "), x[1].strip("' ")))

unofficial10 = country_lang.filter(lambda x: x[2] == 'F').\
    map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x, y: x + y).\
    filter(lambda x: len(x[1]) >= 10).sortBy(lambda x: -1 * len(x[1])).keys().collect()

unofficial = country.filter(lambda x: x[0] in unofficial10).collectAsMap()

for u in unofficial10:
    print(unofficial[u])
