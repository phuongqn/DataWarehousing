from pyspark import SparkContext

sc = SparkContext(master = 'local[*]', appName = 'avgexp')

country = sc.textFile('country.csv').map(lambda x: x.split(',')).map(lambda x: [s.strip("' '").strip("'") for s in x]).\
    map(lambda x: (x[2], x[7], x[8])).filter(lambda x: float(x[2]) > 10000).map(lambda x: (x[0], [float(x[1])]))

avgexp= country.reduceByKey(lambda x, y: x + y).filter(lambda x: len(x[1]) >= 5).map(lambda x: (x[0], sum(x[1]) / len(x[1]))).collect()

for a in avgexp:
    print(a[0] + ',', a[1])
