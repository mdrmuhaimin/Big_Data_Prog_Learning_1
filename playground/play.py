from pyspark import SparkConf, SparkContext
import sys
import json
import re
conf = SparkConf().setAppName('reddit averages')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

def reducer (a, b):
    print(a)
    print(b)
    return a + b

sample = sc.parallelize([('hello', 1), ('alpha', 2), ('alpha', 3)])
sample = sample.reduceByKey(reducer)
print(sample.collect())