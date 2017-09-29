from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

lines = sc.textFile("data.txt")
print( lines.collect())
pairs = lines.map(lambda s: (s, 1))
counts = pairs.reduceByKey(lambda a, b: a + b)
print(counts.collect())


# print(sc.textFile("data.txt").map(lambda s: len(s)).reduce(lambda a, b: a + b))
print(sc.parallelize([("mary", 1), ("has", 1), ("mary", 1)]).reduceByKey(operator.add).collect())
print(sc.parallelize([2, 5, 16, 7, 8]).reduce(lambda x, y: x + y))

