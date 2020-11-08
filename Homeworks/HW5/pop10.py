import sys
from pyspark import SparkContext

continent = sys.argv[1]

sc = SparkContext(master = 'local[*]', appName = 'pop10')

country = sc.textFile('country.csv').map(lambda x: x.split(',')).map(lambda x: [s.strip("' '").strip("'") for s in x])
pop10= country.map(lambda x: (x[2], (x[6], x[1]))).filter(lambda x: x[0] == continent).map(lambda x: (int(x[1][0]), x[1][1])).\
    takeOrdered(num = 10, key = lambda x: -1 * x[0])

for p in pop10:
    print(p[1] + ',' , p[0])
