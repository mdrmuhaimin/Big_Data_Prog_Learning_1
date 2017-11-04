from pyspark import SparkConf, SparkContext
import sys
import re
import math

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('reddit averages').set('spark.dynamicAllocation.maxExecutors', 20)
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.2'  # make sure we have Spark 2.2+

